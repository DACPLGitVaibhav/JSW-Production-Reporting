
using Azure;
using Microsoft.Extensions.Options;
using Opc.Ua;
using Opc.Ua.Client;
using Serilog;
using System.Collections.Concurrent;
using System.Data;
using System.Net.NetworkInformation;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using WS_Haimdall;
using WS_Haimdall.Model_Class;
using static WS_Haimdall.Worker;
using static WS_Haimdall.NodeIdConfig;
using Opc.Ua.Security.Certificates;

namespace WS_Haimdall
{

    public class Worker : BackgroundService
    {
        #region Varriables

        //// 1. Read NodeIds from SQL
        public Dictionary<string, string> nodeIds_Sub = new();

        ////  private static OpcUaClient opcClient;
        private static BusinessLayer bl;

        private static Session session;

        private Dictionary<string, NodeConfg> _nodeConfigs = new();

        private readonly ILogger<Worker> _logger;
        private PeriodicTimer? _timer;
        private readonly appSettings _settings;
        private object lockObj = new object();
        private SessionReconnectHandler reconnectHandler = null;

        //private static readonly SemaphoreSlim _fileLock = new(1, 1);
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _fileLocks_Oth = new();

        //////for alarms
        private Subscription _alarmSubscription;
        private ConcurrentDictionary<string, string> lastAlarmCache = new();
        private static readonly ConcurrentQueue<AlarmData> alarmQueue = new();
        private readonly SemaphoreSlim _signalAlarm = new(0);

        //////for cycle
        private Subscription _cycleSubscription;
        private ConcurrentDictionary<string, string> lastCTCache = new();
        private static readonly ConcurrentQueue<CycleData> CTQueue = new();
        private readonly SemaphoreSlim _CTSignal = new(0);

        /// <summary>
        /// for Cycle time Line CT subscriptions
        /// </summary>
        private Subscription _cycleTimeLineCTSubscription;
        private ConcurrentDictionary<string, string> lastLineCTCache = new();
        private static readonly ConcurrentQueue<CycleTimeLineCTData> lineCTQueue = new();
        private readonly SemaphoreSlim _LineCTSignal = new(0);


        /// <summary>
        /// for Cycle time Substation CT subscriptions
        /// </summary>
        private Subscription _cycleTimeSubStationCTSubscription;
        private ConcurrentDictionary<string, string> lastSubStationCTCache = new();
        private static readonly ConcurrentQueue<CycleTimeSubStaionCTData> SubStationCTQueue = new();
        private readonly SemaphoreSlim _SubStationCTSignal = new(0);


        private static Dictionary<string, string> tagDict;
        #endregion

        public Worker(ILogger<Worker> logger, IOptions<appSettings> options)
        {
            _logger = logger;
            _settings = options.Value;
            bl = new BusinessLayer(_settings.DB_Connection);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await OtherLogs("Success: Service Started.");
           
            bl.FillAlarmMaster();
            bl.FillNodeIdConfig();

            await ConnectOPCSession();

            var alarmTask = InsertAlarm(stoppingToken);
            var ctTask = InsertCT(stoppingToken);
            var lineCtTask = InsertLineCT(stoppingToken);
            var subStTask = InsertSubStationCT(stoppingToken);

            await Task.WhenAll(alarmTask, ctTask, subStTask);
           
        }



        #region OPCUA_Con
        public async Task ConnectOPCSession()

        {
            #region EventBased
            try
            {
                //var endpointUrl = ConfigurationManager.AppSettings["Endpoint"].ToString(); //"opc.tcp://192.168.196.1:4840" Replace with your server's endpoint URL
                var endpointUrl = "opc.tcp://192.168.1.53:4840"; //"opc.tcp://192.168.196.1:4840" Replace with your server's endpoint URL //_settings.Endpoint

                Utils.SetTraceOutput(Utils.TraceOutput.Off);
                var config = new ApplicationConfiguration()
                {
                    ServerConfiguration = new ServerConfiguration
                    {
                        UserTokenPolicies = new UserTokenPolicyCollection(new[] { new UserTokenPolicy(UserTokenType.UserName) }),
                    },
                    ApplicationName = "MyConfig",
                    ApplicationType = ApplicationType.Client,
                    SecurityConfiguration = new SecurityConfiguration
                    {
                        ApplicationCertificate = new CertificateIdentifier
                        {
                            StoreType = @"Windows",
                            StorePath = @"CurrentUser\My",
                            SubjectName = Utils.Format(@"CN={0}, DC={1}", "MyHomework", System.Net.Dns.GetHostName())
                        },
                        TrustedPeerCertificates = new CertificateTrustList
                        {
                            StoreType = @"Windows",
                            StorePath = @"CurrentUser\TrustedPeople",
                        },
                        NonceLength = 32,
                        AutoAcceptUntrustedCertificates = true
                    },
                    //TransportConfigurations = new TransportConfigurationCollection(),
                    //TransportQuotas = new TransportQuotas { OperationTimeout = 15000 },
                    ClientConfiguration = new ClientConfiguration { }
                };

                //_ = config.Validate(ApplicationType.Client);
                //if (config.SecurityConfiguration.AutoAcceptUntrustedCertificates)
                //{
                //    config.CertificateValidator.CertificateValidation += (s, e) => { e.Accept = true; };
                //}

                config.CertificateValidator = new CertificateValidator();
                config.CertificateValidator.CertificateValidation += (s, certificateValidationEventArgs) =>
                {
                    certificateValidationEventArgs.Accept = true; // Accept all certificates for testing purposes; modify this for production.
                };

                // Create a new session with the OPC UA server asynchronously
                session = await Session.Create(config, new ConfiguredEndpoint(null, new EndpointDescription(endpointUrl)), true, "", 60000, new UserIdentity(), null);
                //session.SessionClosing += Session_SessionClosing;
                if (session.Connected)
                {
                    session.KeepAlive += _opcSession_KeepAlive;

                    await OtherLogs("Success: Session Created.");
                    Console.WriteLine("PLC Connected!!!");


                    #region Single Event
                    CreateAlarmSubscription();
                    
                    CreateCycleTime_LineCTSubscription();

                    CreateCycleTime_SubstationCTSubscription();
                    #endregion
                }
                

            }
            catch (ServiceResultException ex)
            {
                await OtherLogs("Error at ServiceResultException" + ex.Message);
                // Handle session creation errors, e.g., invalid session ID or server errors
            }

            catch (TimeoutException ex)
            {
                await OtherLogs("Error at TimeoutException: " + ex.Message);
                // Handle timeout errors, e.g., server not responding in time
            }
            catch (Exception ex)
            {
                await OtherLogs("Error at ConnectOPCSession" + ex.Message);
                //bl.Insert_ErrorLog("ConnectOPCSession", ex.Message, ex.StackTrace);

            }
            #endregion
        }
        #endregion

        #region Subscription's      
        private async void CreateAlarmSubscription()
        {
            _alarmSubscription = new Subscription(session.DefaultSubscription)
            {
                PublishingInterval = 1000,
                DisplayName = "SQLTagSubscription",
                PublishingEnabled = true,
                MaxNotificationsPerPublish = 0 // unlimited
            };

            session.AddSubscription(_alarmSubscription);
            await _alarmSubscription.CreateAsync();

            var monitoredItems = new List<Opc.Ua.Client.MonitoredItem>();

            if(AlarmTags.dict_AlarmTags.Any())
            {
                foreach(var eachItem in AlarmTags.dict_AlarmTags)
                {
                    string nodeIdStr = eachItem.Value;
                    var item = new Opc.Ua.Client.MonitoredItem(_alarmSubscription.DefaultItem)
                    {
                        DisplayName = eachItem.Key.ToString(),// nodeIdStr,
                        StartNodeId = new NodeId(nodeIdStr),
                        AttributeId = Attributes.Value,
                        SamplingInterval = 500,
                        QueueSize = 10,
                        DiscardOldest = true
                    };

                    // Correct way to attach the notification handler
                    item.Notification += OnAlarmTriggered;

                    monitoredItems.Add(item);
                }
            }
           
            _alarmSubscription.AddItems(monitoredItems);

            await _alarmSubscription.ApplyChangesAsync();
        }

        //Line CT Subscriptions method
        private async Task CreateCycleTime_LineCTSubscription()
        {
            _cycleTimeLineCTSubscription = new Subscription(session.DefaultSubscription)
            {
                PublishingInterval = 250,
                DisplayName = "CycleTriggerSubscription",
                PublishingEnabled = true
            };

            session.AddSubscription(_cycleTimeLineCTSubscription);
            await _cycleTimeLineCTSubscription.CreateAsync();

            var monitoredItems = new List<Opc.Ua.Client.MonitoredItem>();

            foreach (var eachItem in NodeIdConfig.dict_NodeIdConfigLineCT)
            {
                if (!eachItem.Key.Contains("_BiwNo"))
                    continue;

                string nodeIdStr = eachItem.Value;
                var item = new Opc.Ua.Client.MonitoredItem(_cycleTimeLineCTSubscription.DefaultItem)
                {
                    DisplayName = eachItem.Key.ToString(),// nodeIdStr,
                    StartNodeId = new NodeId(nodeIdStr),
                    AttributeId = Attributes.Value,
                    SamplingInterval = 500,
                    QueueSize = 10,
                    DiscardOldest = true
                };

                // Correct way to attach the notification handler
                item.Notification += OnCycleTimeLineCTTrigger;

                monitoredItems.Add(item);
            }


            _cycleTimeLineCTSubscription.AddItems(monitoredItems);

            await _cycleTimeLineCTSubscription.ApplyChangesAsync();
        }

        //Substation CT Subscriptions method
        private async Task CreateCycleTime_SubstationCTSubscription()
        {
            _cycleTimeSubStationCTSubscription = new Subscription(session.DefaultSubscription)
            {
                PublishingInterval = 250,
                DisplayName = "CycleTriggerSubscription",
                PublishingEnabled = true
            };

            session.AddSubscription(_cycleTimeSubStationCTSubscription);
            await _cycleTimeSubStationCTSubscription.CreateAsync();

            var monitoredItems = new List<Opc.Ua.Client.MonitoredItem>();

            foreach (var eachItem in NodeIdConfig.dict_NodeIdConfigSubstationCT)
            {
                if (!eachItem.Key.Contains("_BiwNo"))
                    continue;

                string nodeIdStr = eachItem.Value;
                var item = new Opc.Ua.Client.MonitoredItem(_cycleTimeSubStationCTSubscription.DefaultItem)
                {
                    DisplayName = eachItem.Key.ToString(),// nodeIdStr,
                    StartNodeId = new NodeId(nodeIdStr),
                    AttributeId = Attributes.Value,
                    SamplingInterval = 500,
                    QueueSize = 10,
                    DiscardOldest = true
                };

                // Correct way to attach the notification handler
                item.Notification += OnCycleTimeSubstationCTTrigger;

                monitoredItems.Add(item);
            }


            _cycleTimeSubStationCTSubscription.AddItems(monitoredItems);

            await _cycleTimeSubStationCTSubscription.ApplyChangesAsync();
        }



        private async Task CreateCycleTriggerSubscription()
        {
            _cycleSubscription = new Subscription(session.DefaultSubscription)
            {
                PublishingInterval = 250,
                DisplayName = "CycleTriggerSubscription",
                PublishingEnabled = true
            };

            session.AddSubscription(_cycleSubscription);
            await _cycleSubscription.CreateAsync();

            string nodeIdStr = $"ns=3;s=\"CycleTime\".\"FIFO1\".\"Item_ID\"";

            var item = new MonitoredItem(_cycleSubscription.DefaultItem)
            {
                DisplayName = "RF_RF010GF_Item_ID",
                StartNodeId = new NodeId(nodeIdStr),
                AttributeId = Attributes.Value,
                SamplingInterval = 250,
                QueueSize = 5,
                DiscardOldest = true
            };

            item.Notification += OnCycleTrigger;

            _cycleSubscription.AddItem(item);
            await _cycleSubscription.ApplyChangesAsync();

        }
        #endregion

        #region Trigger's      
        private void OnAlarmTriggered(Opc.Ua.Client.MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            bool added = false;
            #region New
            //lock (queueLock) // freeze during DB operation
            //{
            foreach (var value in item.DequeueValues())
            {
                string tag = item.DisplayName;
                string val = value.Value?.ToString();

                // 🔥 Check duplicate
                if (lastAlarmCache.TryGetValue(tag, out var lastVal))
                {
                    if (lastVal == val)
                        continue; // ❌ skip duplicate
                }

                // ✅ update cache
                lastAlarmCache[tag] = val;

                var alarm = new AlarmData
                {
                    AlarmCode = item.DisplayName,
                    Value = value.Value?.ToString(),
                    Action = Convert.ToBoolean(value.Value) ? "HIGH" : "LOW",
                    Timestamp = value.SourceTimestamp.ToLocalTime()
                };

                alarmQueue.Enqueue(alarm);
                added = true;

            }
            //}

            if (added)
                _signalAlarm.Release(); // 🔥 release once per batch
            #endregion


        }

        private async void OnCycleTrigger(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            foreach (var value in item.DequeueValues())
            {
                string tag = item.DisplayName;
                var itemId = value.Value?.ToString();

                if (string.IsNullOrEmpty(itemId) || itemId == "0")
                    return;



                // 🔥 Check duplicate
                if (lastCTCache.TryGetValue(tag, out var lastVal))
                {
                    if (lastVal == itemId)
                        continue; // ❌ skip duplicate
                }

                // ✅ update cache
                lastCTCache[tag] = itemId;
                //////////////////////////
                ///
                try
                {
                    var cycleData = await ReadCycleData();
                    if (cycleData != null)
                    {
                        cycleData.TagName = tag;
                        cycleData.ItemId = itemId;
                        cycleData.Timestamp = value.SourceTimestamp.ToLocalTime();

                        CTQueue.Enqueue(cycleData);
                        _CTSignal.Release();
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error reading cycle data");
                }
            }
        }


        private async void OnCycleTimeLineCTTrigger(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            foreach (var value in item.DequeueValues())
            {
                string tag = item.DisplayName;
                var itemId = value.Value?.ToString();

                if (string.IsNullOrEmpty(itemId) || itemId == "0")
                    return;



                // 🔥 Check duplicate
                if (lastLineCTCache.TryGetValue(tag, out var lastVal))
                {
                    if (lastVal == itemId)
                        continue; // ❌ skip duplicate
                }

                // ✅ update cache
                lastLineCTCache[tag] = itemId;
                //////////////////////////
                ///
                try
                {
                    var cycleTimeLineCTData = await ReadCycleTimeLineCTData(tag);
                    if (cycleTimeLineCTData != null)
                    {
                        lineCTQueue.Enqueue(cycleTimeLineCTData);
                        _LineCTSignal.Release();
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error reading cycle data");
                }
            }
        }

        private async void OnCycleTimeSubstationCTTrigger(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            foreach (var value in item.DequeueValues())
            {
                string tag = item.DisplayName;



                var itemId = value.Value?.ToString();

                if (string.IsNullOrEmpty(itemId) || itemId == "0")
                    return;



                // 🔥 Check duplicate
                if (lastSubStationCTCache.TryGetValue(tag, out var lastVal))
                {
                    if (lastVal == itemId)
                        continue; // ❌ skip duplicate
                }

                // ✅ update cache
                lastSubStationCTCache[tag] = itemId;
                //////////////////////////
                ///
                try
                {
                    var cycleTimeSubStationCTData = await ReadCycleTimeSubStationCTData(tag);
                    if (cycleTimeSubStationCTData != null)
                    {
                        SubStationCTQueue.Enqueue(cycleTimeSubStationCTData);
                        _SubStationCTSignal.Release();
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error reading cycle data");
                }
            }
        }
        #endregion

        #region DataRead       
        private async Task<CycleData?> ReadCycleData()
        {
            var nodesToRead = new ReadValueIdCollection
                {
                    new ReadValueId { NodeId = new NodeId($"ns=3;s=\"CycleTime\".\"FIFO1\".\"BIW_No\""), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = new NodeId($"ns=3;s=\"CycleTime\".\"FIFO1\".\"Model_Code\""), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = new NodeId($"ns=3;s=\"CycleTime\".\"FIFO1\".\"Cycle_Time\""), AttributeId = Attributes.Value }
                };

            var results = await session.ReadAsync(
                null,
                0,
                TimestampsToReturn.Source,
                nodesToRead,
                CancellationToken.None
            );

            bool isAllGood = results.Results.All(r => StatusCode.IsGood(r.StatusCode));

            if (!isAllGood)
                return null;

            return new CycleData
            {

                BiwNo = results.Results[0].Value?.ToString(),
                ModelCode = results.Results[1].Value?.ToString(),
                CycleTime = Convert.ToDouble(results.Results[2].Value)
            };


        }

        private async Task<CycleTimeLineCTData?> ReadCycleTimeLineCTData(string key)
        {
            try
            {
                var keyArray = key.Split("_");

                var Id = keyArray[0];
                var line = keyArray[1];


                var ke = $"{Id}_{line}_SubVariant";

                if (dict_NodeIdConfigLineCT.TryGetValue(key, out string valu))
                {

                }

                var nodesToRead = new ReadValueIdCollection
                {
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineCT[$"{Id}_{line}_StartTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineCT[$"{Id}_{line}_EndTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineCT[$"{Id}_{line}_CycleTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineCT[$"{Id}_{line}_BiwNo"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineCT[$"{Id}_{line}_Variant"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineCT[$"{Id}_{line}_SubVariant"]), AttributeId = Attributes.Value }
                };


                var results = await session.ReadAsync(
                    null,
                    0,
                    TimestampsToReturn.Source,
                    nodesToRead,
                    CancellationToken.None
                );

                bool isAllGood = results.Results.All(r => StatusCode.IsGood(r.StatusCode));

                if (!isAllGood)
                    return null;

                return new CycleTimeLineCTData
                {
                    LineID = Convert.ToInt32(Id),
                    StartTime = ConvertPlcDateTime((byte[])results.Results[0].Value),
                    EndTime = ConvertPlcDateTime((byte[])results.Results[1].Value),
                    CycleTime = Convert.ToInt32(results.Results[2].Value),
                    Biwno = Convert.ToString(results.Results[3].Value),
                    VarriantCode = Convert.ToInt32(results.Results[4].Value),
                    SubVarraintcode = Convert.ToInt32(results.Results[5].Value),
                    TimeStamp = ConvertPlcDateTime((byte[])results.Results[1].Value),
                };
            }
            catch (Exception ex)
            {
                return null;
            }
            
        }

        private async Task<CycleTimeSubStaionCTData?> ReadCycleTimeSubStationCTData(string key)
        {
            try
            {
                var keyArray = key.Split('_');

                var Id = keyArray[0];
                var subStation = keyArray[1];

                var nodesToRead = new ReadValueIdCollection
                {
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_StartTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_EndTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_CycleTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_BiwNo"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_Variant"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_SubVariant"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_Emergency"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_TipChange"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_TipDress"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_OperatorLoadingStarvingTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_Block_Time"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_Manual"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_PartPresentFault"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_RollMoveTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_LifterMoveTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_TurnTableMoveTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_ClampTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_DeclampTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_MarriageMissMatch"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_DropTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_WeldTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_PickTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_SealingTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_SafetyGate"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_Miscellaneous"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_MaterialCall"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_Others"]), AttributeId = Attributes.Value }
                };

                var results = await session.ReadAsync(
                    null,
                    0,
                    TimestampsToReturn.Source,
                    nodesToRead,
                    CancellationToken.None
                );

                bool isAllGood = results.Results.All(r => StatusCode.IsGood(r.StatusCode));

                if (!isAllGood)
                    return null;

                var st = ConvertPlcDateTime((byte[])results.Results[0].Value);
                var et = ConvertPlcDateTime((byte[])results.Results[1].Value);

                return new CycleTimeSubStaionCTData
                {
                    StartTime = ConvertPlcDateTime((byte[])results.Results[0].Value),
                    EndTime = ConvertPlcDateTime((byte[])results.Results[1].Value),
                    CycleTime = Convert.ToInt32(results.Results[2].Value),
                    Biwno = results.Results[3].Value.ToString(),
                    VarraintCode = Convert.ToInt32(results.Results[4].Value),
                    SubVarraintcode = Convert.ToInt32(results.Results[5].Value),
                    Emergency = Convert.ToInt32(results.Results[6].Value),
                    Tip_Change = Convert.ToInt32(results.Results[7].Value),
                    Tip_Dress = Convert.ToInt32(results.Results[8].Value),
                    OperatorLoading_Starving_Time = Convert.ToInt32(results.Results[9].Value),
                    Block_Time = Convert.ToInt32(results.Results[10].Value),
                    Manual = Convert.ToInt32(results.Results[11].Value),
                    Part_Present_Fault = Convert.ToInt32(results.Results[12].Value),
                    RollMoveTime = Convert.ToInt32(results.Results[13].Value),
                    LifterMoveTime = Convert.ToInt32(results.Results[14].Value),
                    TurnTableMoveTime = Convert.ToInt32(results.Results[15].Value),
                    ClampTime = Convert.ToInt32(results.Results[16].Value),
                    DeclampTime = Convert.ToInt32(results.Results[17].Value),
                    Marriage_Miss_Match = Convert.ToInt32(results.Results[18].Value),
                    DropTime = Convert.ToInt32(results.Results[19].Value),
                    WeldTime = Convert.ToInt32(results.Results[20].Value),
                    PickTime = Convert.ToInt32(results.Results[21].Value),
                    SealingTime = Convert.ToInt32(results.Results[22].Value),
                    Safety_Gate = Convert.ToInt32(results.Results[23].Value),
                    Miscellaneous = Convert.ToInt32(results.Results[24].Value),
                    MaterialCall = Convert.ToInt32(results.Results[25].Value),
                    Others = Convert.ToInt32(results.Results[26].Value),
                    TimeStamp = ConvertPlcDateTime((byte[])results.Results[1].Value),
                    Sub_StationID = Convert.ToInt32(Id)

                };

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.ToString());
                return null;
            }

           

        }


        //private DateTime ConvertPlcDateTime(byte[] bytes)
        //{
        //    if (bytes == null || bytes.Length < 6)
        //        throw new ArgumentException("Invalid PLC DateTime");

        //    int year = 2000 + bytes[0];
        //    int month = bytes[1];
        //    int day = bytes[2];

        //    int hour = bytes[3];
        //    int minute = bytes[4];
        //    int second = bytes[5];

        //    return new DateTime(
        //        year,
        //        month,
        //        day,
        //        hour,
        //        minute,
        //        second);
        //}

        private DateTime ConvertPlcDateTime(byte[] bytes)
        {
            if (bytes == null || bytes.Length < 6)
                throw new ArgumentException("Invalid PLC DateTime");

            int year = 2000 + BcdToInt(bytes[0]);
            int month = BcdToInt(bytes[1]);
            int day = BcdToInt(bytes[2]);

            int hour = BcdToInt(bytes[3]);
            int minute = BcdToInt(bytes[4]);
            int second = BcdToInt(bytes[5]);

            return new DateTime(
                year,
                month,
                day,
                hour,
                minute,
                second);
        }

        private int BcdToInt(byte value)
        {
            return ((value >> 4) * 10) + (value & 0x0F);
        }
        #endregion

        #region DBInsertion       
        private async Task InsertAlarm(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _signalAlarm.WaitAsync(stoppingToken); // ⏳ wait for data


                    List<AlarmData> batch = new();/* int maxBatchSize = 2000;*/

                    //lock (queueLock) // 🔒 FREEZE QUEUE
                    //{
                    while (alarmQueue.TryDequeue(out var alarm))
                    {
                        batch.Add(alarm);
                    }
                    //}

                    if (batch.Count == 0)
                    {
                        await Task.Delay(500);
                        continue;
                    }


                    if (batch.Count > 0)
                    {
                        string jsonString = JsonSerializer.Serialize(batch);
                        await bl.InsertAlarm(jsonString);
                        //Console.WriteLine($"{batch[0].TagName} | {batch[0].Value} | {batch[0].Timestamp}");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in background worker.");
                }

            }
        }

        private async Task InsertCT(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _CTSignal.WaitAsync(stoppingToken); // ⏳ wait for data


                    List<CycleData> batch = new();/* int maxBatchSize = 2000;*/

                    //lock (queueLock) // 🔒 FREEZE QUEUE
                    //{
                    while (CTQueue.TryDequeue(out var CT))
                    {
                        batch.Add(CT);
                    }
                    //}

                    if (batch.Count == 0)
                    {
                        await Task.Delay(500);
                        continue;
                    }


                    if (batch.Count > 0)
                    {
                        string jsonString = JsonSerializer.Serialize(batch);
                        await bl.InsertCT(jsonString);
                        //Console.WriteLine($"{batch[0].TagName} | {batch[0].Value} | {batch[0].Timestamp}");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in background worker.");
                }

            }
        }


        private async Task InsertLineCT(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _LineCTSignal.WaitAsync(stoppingToken); // ⏳ wait for data


                    List<CycleTimeLineCTData> batch = new();/* int maxBatchSize = 2000;*/

                    //lock (queueLock) // 🔒 FREEZE QUEUE
                    //{
                    while (lineCTQueue.TryDequeue(out var CT))
                    {
                        batch.Add(CT);
                    }
                    //}

                    if (batch.Count == 0)
                    {
                        await Task.Delay(500);
                        continue;
                    }


                    if (batch.Count > 0)
                    {
                        string jsonString = JsonSerializer.Serialize(batch);
                        await bl.InsertLineCT(jsonString);
                        //Console.WriteLine($"{batch[0].TagName} | {batch[0].Value} | {batch[0].Timestamp}");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in background worker.");
                }

            }
        }

        private async Task InsertSubStationCT(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _SubStationCTSignal.WaitAsync(stoppingToken); 


                    List<CycleTimeSubStaionCTData> batch = new();

                   
                    while (SubStationCTQueue.TryDequeue(out var SubSt))
                    {
                        batch.Add(SubSt);
                    }


                    if (batch.Count == 0)
                    {
                        await Task.Delay(500);
                        continue;
                    }


                    if (batch.Count > 0)
                    {
                        string jsonString = JsonSerializer.Serialize(batch);
                        await bl.InsertSubStationCT(jsonString);
                        //Console.WriteLine($"{batch[0].TagName} | {batch[0].Value} | {batch[0].Timestamp}");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in background worker.");
                }

            }
        }

        #endregion

        #region OPCUA_RecCon        
        private async void _opcSession_KeepAlive(ISession session, KeepAliveEventArgs e)
        {
            try
            {
                if (e.Status != null && ServiceResult.IsNotGood(e.Status))
                {
                    await OtherLogs("Error: PLC Disconnected: " + e.Status);

                    lock (lockObj)
                    {
                        if (reconnectHandler == null)
                        {
                            reconnectHandler = new SessionReconnectHandler();

                            reconnectHandler.BeginReconnect(
                                session,
                                3000,
                                Client_ReconnectComplete
                            );

                            OtherLogs("Error: trying to Reconnecting...");
                        }
                    }
                }
            }
            catch (Exception)
            {

            }
        }

        private void Client_ReconnectComplete(object? sender, EventArgs e)
        {
            lock (lockObj)
            {
                if (sender is SessionReconnectHandler handler)
                {
                    session = (Session)handler.Session;
                    reconnectHandler = null;

                    OtherLogs("PLC Reconnected Successfully!");
                }
            }
        }
        #endregion
        private bool PingHost()
        {
            bool pingable = false;
            System.Net.NetworkInformation.Ping pinger = null;
            try
            {
                pinger = new System.Net.NetworkInformation.Ping();
                PingReply reply = pinger.Send(_settings.IP.ToString());
                pingable = reply.Status == IPStatus.Success;
            }
            catch (PingException ex)
            {
                OtherLogs("Error at PingHost() " + ex.Message);
                //bl.Insert_ErrorLog("PingHost", ex.Message, ex.StackTrace);
            }
            finally
            {
                if (pinger != null)
                {
                    pinger.Dispose();
                }
            }

            return pingable;
        }

        private async Task<bool> NodeConfgg()
        {
            bool b = true;

            #region New
            //try
            //{
            //    DataSet dsNodeID = bl.GetNodeID();
            //    DataTable dtNodeID = dsNodeID.Tables[0];

            //    if (dtNodeID.Rows.Count > 0)
            //    {
            //        foreach (DataRow row in dtNodeID.Rows)
            //        {
            //            //string lineName = row["LineID"].ToString(); // IMPORTANT

            //            var config = new NodeConfg
            //            {
            //                LineID = Convert.ToInt32(row["LineID"]?.ToString()),
            //                LineName = row["LineName"].ToString(),
            //                Itemid_NodeId = row["Itemid_NodeId"]?.ToString(),
            //                Biwno_NodeId = row["Biwno_NodeId"]?.ToString(),
            //                Seqno_NodeId = row["Seqno_NodeId"]?.ToString(),
            //                Modelcode_NodeId = row["Modelcode_NodeId"]?.ToString(),
            //                Data_Received_NodeId = row["Data_Received_NodeId"]?.ToString(),
            //                LOT_Seqno = row["LOT_Seqno"]?.ToString(),
            //                LOS_Seqno = row["LOS_Seqno"]?.ToString(),
            //                LOP_Seqno = row["LOP_Seqno"]?.ToString(),
            //                PRG_Empty = row["PRG_Empty"]?.ToString(),
            //                OmsLineStatus = row["OmsLineStatus"]?.ToString()
            //            };

            //            _nodeConfigs[config.LineID.ToString()] = config;

            //            nodeIds_Sub.Add($"{config.LineID}_{config.LineName}_Started", config.LOS_Seqno.ToString());
            //            nodeIds_Sub.Add($"{config.LineID}_{config.LineName}_Processed", config.LOP_Seqno.ToString());
            //        }
            //        b = true;
            //    }
            //    else
            //    {
            //        b = false;
            //        await OtherLogs("Warning configure the tag addresses first.");
            //    }
            //}
            //catch (Exception)
            //{
            //    b = false;
            //    await OtherLogs("Warning while configure tag addresses.");
            //}
            #endregion

            return b;
        }
        #region LogFile
        private async Task OtherLogs(string s)
        {
            #region New
            try
            {
                string currentDirectory = Path.Combine(_settings.LogPath, "OtherLogs");
                Directory.CreateDirectory(currentDirectory);

                string filePath = Path.Combine(currentDirectory, "OtherLogs.txt");

                // Get or create lock for this specific line
                var fileLock = _fileLocks_Oth.GetOrAdd("OtherLogs", _ => new SemaphoreSlim(1, 1));

                await fileLock.WaitAsync();

                try
                {
                    // Append log asynchronously
                    using (var stream = new FileStream(
                        filePath,
                        FileMode.Append,
                        FileAccess.Write,
                        FileShare.Read,
                        4096,
                        useAsync: true))
                    using (var writer = new StreamWriter(stream))
                    {
                        await writer.WriteLineAsync($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} {s}");
                    }

                    // Check file size
                    FileInfo fileInfo = new FileInfo(filePath);
                    long maxSizeInBytes = 1_000_000;

                    if (fileInfo.Exists && fileInfo.Length > maxSizeInBytes)
                    {
                        string newFileName = $"OtherLogs_{DateTime.Now:yyyyMMdd_HHmmss}.txt";

                        string newFilePath = Path.Combine(currentDirectory, newFileName);

                        File.Move(filePath, newFilePath);
                    }
                }
                finally
                {
                    fileLock.Release();
                }
            }
            catch
            {
                // log silently or handle properly
            }
            #endregion

        }


        #endregion

        public override async Task StopAsync(CancellationToken cancellationToken)
        {

            await OtherLogs("Service Stopping...");

            _timer?.Dispose();   // 🔥 Dispose timer here

            await session.CloseAsync(); //Session close
            session.Dispose();  //Session Dispose
            session = null; //Session null

            // Add your cleanup logic here
            // Example: close DB, dispose resources, stop timers

            await base.StopAsync(cancellationToken);

            await OtherLogs("Service Stopped.");
        }

        private void LoadDic()
        {
            tagDict = new Dictionary<string, string>
                {
                    { "RF_RF010GF_ItemID", "ns=3;s=\"CycleTime\".\"FIFO1\".\"Item_ID\"" },
                    { "RF_RF010GF_BiwNo", "ns=3;s=\"CycleTime\".\"FIFO1\".\"BIW_No\"" },
                    { "RF_RF010GF_ModelCode", "ns=3;s=\"CycleTime\".\"FIFO1\".\"Model_Code\"" },
                    { "RF_RF010GF_CycleTime", "ns=3;s=\"CycleTime\".\"FIFO1\".\"Cycle_Time\"" },

                    { "RF_RF020GF_ItemID", "ns=3;s=\"CycleTime\".\"FIFO1\".\"Item_ID\"" },
                    { "RF_RF020GF_BiwNo", "ns=3;s=\"CycleTime\".\"FIFO1\".\"BIW_No\"" },
                    { "RF_RF020GF_ModelCode", "ns=3;s=\"CycleTime\".\"FIFO1\".\"Model_Code\"" },
                    { "RF_RF020GF_CycleTime", "ns=3;s=\"CycleTime\".\"FIFO1\".\"Cycle_Time\"" }
                };

            var stationMap = tagDict
                    .GroupBy(x => x.Key.Substring(0, x.Key.LastIndexOf("_")))
                    .ToDictionary(
                        g => g.Key,
                        g => g.ToDictionary(
                            x => x.Key.Substring(x.Key.LastIndexOf("_") + 1),
                            x => x.Value
                        )
                    );

            foreach (var station in stationMap)
            {
                if (!station.Value.ContainsKey("ItemID"))
                    continue;

                string StartNodeId = station.Value["ItemID"];
                string DisplayName = station.Key;

                //var monitoredItem = new MonitoredItem(subscription.DefaultItem)
                //{
                //    StartNodeId = station.Value["ItemID"],
                //    DisplayName = station.Key
                //};

                //monitoredItem.Notification += (m, e) =>
                //    OnItemChanged(m, e, station.Key);

                //subscription.AddItem(monitoredItem);

                #region Event


                //////Event
                //private async void OnItemChanged(MonitoredItem item,  MonitoredItemNotificationEventArgs e, string station)
                // {
                //     foreach (var val in item.DequeueValues())
                //     {
                //         var itemId = val.Value?.ToString();

                //         if (string.IsNullOrEmpty(itemId))
                //             return;

                //         var data = await ReadStationAsync(station);

                //         Console.WriteLine($"[{station}] " +
                //             $"Item:{data.GetValueOrDefault("ItemID")}, " +
                //             $"BIW:{data.GetValueOrDefault("BiwNo")}, " +
                //             $"Model:{data.GetValueOrDefault("ModelCode")}, " +
                //             $"CT:{data.GetValueOrDefault("CycleTime")}");
                //     }
                // }
                #endregion

                #region BatchRead


                //////BatchRead
                //private async Task<Dictionary<string, object>> ReadStationAsync(string station)
                //{
                //        var tags = stationMap[station];

                //        var nodesToRead = new ReadValueIdCollection();

                //        foreach (var tag in tags)
                //        {
                //            nodesToRead.Add(new ReadValueId
                //            {
                //                NodeId = new NodeId(tag.Value),
                //                AttributeId = Attributes.Value
                //            });
                //        }

                //        var result = await _session.ReadAsync(
                //            null,
                //            0,
                //            TimestampsToReturn.Neither,
                //            nodesToRead,
                //            CancellationToken.None);

                //        var output = new Dictionary<string, object>();

                //        int i = 0;
                //        foreach (var key in tags.Keys)
                //        {
                //            output[key] = result.Results[i++].Value;
                //        }

                //        return output;
                //}
                #endregion

                #region Duplicate Prevention


                //////// 
                //Dictionary<string, string> lastItemCache = new();

                //if (lastItemCache.TryGetValue(station, out var lastVal) && lastVal == itemId)
                //    return;

                //lastItemCache[station] = itemId;
                #endregion
            }

        }

        public class AlarmData
        {
            public string AlarmCode { get; set; }
            public object Value { get; set; }
            public string Action { get; set; }
            public DateTime Timestamp { get; set; }
        }

        public class CycleData
        {
            public string? TagName { get; set; }
            public string? BiwNo { get; set; }
            public string? ItemId { get; set; }
            public object? CycleTime { get; set; }
            public string? ModelCode { get; set; }
            public DateTime Timestamp { get; set; }
        }

        public class CycleTimeLineCTData
        {
            public int? Id { get; set; }
            public int? LineID { get; set; }
            public DateTime? StartTime { get; set; }
            public DateTime? EndTime { get; set; }
            public int? CycleTime { get; set; }
            public string? Biwno { get; set; }
            public int? VarriantCode { get; set; }
            public int? SubVarraintcode { get; set; }
            public DateTime? TimeStamp { get; set; }
            
        }

        public class CycleTimeSubStaionCTData
        {
            public int? Id { get; set; }
            public int? LineID { get; set; }

            public DateTime? StartTime { get; set; }
            public DateTime? EndTime { get; set; }

            public int? CycleTime { get; set; }
            public string? Biwno { get; set; }

            public int? VarraintCode { get; set; }
            public int? SubVarraintcode { get; set; }

            public int? Emergency { get; set; }
            public int? Tip_Change { get; set; }
            public int? Tip_Dress { get; set; }
            public int? OperatorLoading_Starving_Time { get; set; }
            public int? Block_Time { get; set; }
            public int? Manual { get; set; }
            public int? Part_Present_Fault { get; set; }
            public int? RollMoveTime { get; set; }
            public int? LifterMoveTime { get; set; }
            public int? TurnTableMoveTime { get; set; }
            public int? ClampTime { get; set; }
            public int? DeclampTime { get; set; }
            public int? Marriage_Miss_Match { get; set; }
            public int? DropTime { get; set; }
            public int? WeldTime { get; set; }
            public int? PickTime { get; set; }
            public int? SealingTime { get; set; }
            public int? Safety_Gate { get; set; }
            public int? Miscellaneous { get; set; }
            public int? MaterialCall { get; set; }
            public int? Others { get; set; }

            public DateTime? TimeStamp { get; set; }

            public int? Sub_StationID { get; set; }
        }


        public class NodeConfg
        {
            public string Itemid_NodeId { get; set; }
            public string Biwno_NodeId { get; set; }
            public string Seqno_NodeId { get; set; }
            public string Modelcode_NodeId { get; set; }
            public string Data_Received_NodeId { get; set; }
            public string LOT_Seqno { get; set; }
            public string LOS_Seqno { get; set; }
            public string LOP_Seqno { get; set; }
            public string PRG_Empty { get; set; }
            public string LineName { get; set; }
            public int LineID { get; set; }
            public string OmsLineStatus { get; set; }
        }


    }
}

