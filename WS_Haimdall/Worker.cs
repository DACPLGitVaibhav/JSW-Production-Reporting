
using Azure;
using Microsoft.Extensions.Options;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Security.Certificates;
using Serilog;
using Serilog;
using Serilog.Core;
using System.Collections.Concurrent;
using System.Data;
using System.Net.NetworkInformation;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using WS_Haimdall;
using WS_Haimdall.Model_Class;
using static WS_Haimdall.Cache.AppCache;
using static WS_Haimdall.Worker;
using WS_Haimdall.Model_Class;

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

        //private readonly ILogger<Worker> _logger;
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

        /// <summary>
        /// for Cycle time Line wise prod data subscriptions
        /// </summary>
        private Subscription _lineWiseProdDataSubscription;
        private ConcurrentDictionary<string, string> lineWiseProdDataCache = new();
        private static readonly ConcurrentQueue<LineWiseProdData> lineWiseProdDataQueue = new();
        private readonly SemaphoreSlim _LineWiseProdDataSignal = new(0);


        private static Dictionary<string, string> tagDict;
        #endregion

        public Worker(ILogger<Worker> logger, IOptions<appSettings> options)
        {
           // _logger = logger;
            _settings = options.Value;
            bl = new BusinessLayer(_settings.DB_Connection);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                Log.Information("Success: Service Started.");

                bl.FillAlarmMaster();
                bl.FillNodeIdConfig();

                await ConnectOPCSession();

                var alarmTask = InsertAlarm(stoppingToken);
                var ctTask = InsertCT(stoppingToken);
                var lineCtTask = InsertLineCT(stoppingToken);
                var subStTask = InsertSubStationCT(stoppingToken);
                var lineWiseProdData = InsertLineWiseProdData(stoppingToken);


                await Task.WhenAll(alarmTask, ctTask, subStTask);
            }
            catch(Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }
            
           
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
                    
                    ClientConfiguration = new ClientConfiguration { }
                };

              

                config.CertificateValidator = new CertificateValidator();
                config.CertificateValidator.CertificateValidation += (s, certificateValidationEventArgs) =>
                {
                    certificateValidationEventArgs.Accept = true; // Accept all certificates for testing purposes; modify this for production.
                };

                // Create a new session with the OPC UA server asynchronously
                session = await Session.Create(config, new ConfiguredEndpoint(null, new EndpointDescription(endpointUrl)), true, "", 60000, new UserIdentity(), null);
           
                if (session.Connected)
                {
                    session.KeepAlive += _opcSession_KeepAlive;

                    Log.Information("Success: Session Created.");

                    #region Single Event
                    //Alarm 
                    CreateAlarmSubscription();
                    
                    //Line CT
                    CreateCycleTime_LineCTSubscription();

                    //SubStation CT
                    CreateCycleTime_SubstationCTSubscription();

                    //Line Wise Prod Data
                    CreateLineWise_ProdDataSubscription();
                    #endregion
                }
                

            }
            catch (ServiceResultException ex)
            {
                Log.Error(ex, "Error at ServiceResultException" + ex.Message);
            }

            catch (TimeoutException ex)
            {
                Log.Error(ex, "Error at TimeoutException: " + ex.Message);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error at ConnectOPCSession" + ex.Message);

            }
            #endregion
        }
        #endregion

        #region Subscription's      
        private async void CreateAlarmSubscription()
        {
            try
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

                if (dict_AlarmTags.Any())
                {
                    foreach (var eachItem in dict_AlarmTags)
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
            catch(Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }

            
        }

        //Line CT Subscriptions method
        private async Task CreateCycleTime_LineCTSubscription()
        {
            try 
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

                foreach (var eachItem in dict_NodeIdConfigLineCT)
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
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }
           
        }

        //Substation CT Subscriptions method
        private async Task CreateCycleTime_SubstationCTSubscription()
        {
            try
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

                foreach (var eachItem in dict_NodeIdConfigSubstationCT)
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
            catch(Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }
            
        }


        //Substation CT Subscriptions method
        private async Task CreateLineWise_ProdDataSubscription()
        {
            try
            {
                _lineWiseProdDataSubscription = new Subscription(session.DefaultSubscription)
                {
                    PublishingInterval = 250,
                    DisplayName = "CycleTriggerSubscription",
                    PublishingEnabled = true
                };

                session.AddSubscription(_lineWiseProdDataSubscription);
                await _lineWiseProdDataSubscription.CreateAsync();

                var monitoredItems = new List<Opc.Ua.Client.MonitoredItem>();

                foreach (var eachItem in dict_NodeIdConfigLineWiseProdData)
                {
                    if (!eachItem.Key.EndsWith("_Actual"))
                        continue;

                    string nodeIdStr = eachItem.Value;
                    var item = new Opc.Ua.Client.MonitoredItem(_lineWiseProdDataSubscription.DefaultItem)
                    {
                        DisplayName = eachItem.Key.ToString(),// nodeIdStr,
                        StartNodeId = new NodeId(nodeIdStr),
                        AttributeId = Attributes.Value,
                        SamplingInterval = 500,
                        QueueSize = 10,
                        DiscardOldest = true
                    };

                    // Correct way to attach the notification handler
                    item.Notification += OnLineWiseProdDataTrigger;

                    monitoredItems.Add(item);
                }


                _lineWiseProdDataSubscription.AddItems(monitoredItems);

                await _lineWiseProdDataSubscription.ApplyChangesAsync();
            }
            catch(Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }
            
        }

        #endregion

        #region Trigger's      
        private void OnAlarmTriggered(Opc.Ua.Client.MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            try
            {
                bool added = false;
                #region New
               
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

                if (added)
                    _signalAlarm.Release(); // 🔥 release once per batch
                #endregion
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }
            
        }


        private async void OnCycleTimeLineCTTrigger(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            try
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
                        Log.Error(ex, "Error reading cycle data");
                    }
                }
            }
            catch(Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }
            
        }

        private async void OnCycleTimeSubstationCTTrigger(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            try
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
                        Log.Error(ex, "Error reading cycle data");
                    }
                }
            }
            catch(Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }
            
        }

        private async void OnLineWiseProdDataTrigger(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            try
            {
                foreach (var value in item.DequeueValues())
                {
                    string tag = item.DisplayName;

                    var itemId = value.Value?.ToString();

                    //if (string.IsNullOrEmpty(itemId) || itemId == "0")
                    //    return;

                    // 🔥 Check duplicate
                    if (lineWiseProdDataCache.TryGetValue(tag, out var lastVal))
                    {
                        if (lastVal == itemId)
                            continue; // ❌ skip duplicate
                    }

                    // ✅ update cache
                    lineWiseProdDataCache[tag] = itemId;
                    //////////////////////////
                    ///
                    try
                    {
                        var lineWiseProdData = await ReadLineWiseProdData(tag);
                        if (lineWiseProdData != null)
                        {
                            lineWiseProdDataQueue.Enqueue(lineWiseProdData);
                            _LineWiseProdDataSignal.Release();
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "Error reading cycle data");
                    }
                }
            }
            catch(Exception ex)
            {
                Log.Error(ex.ToString());
            }
            
        }
        #endregion

        #region DataRead       

        private async Task<CycleTimeLineCTData?> ReadCycleTimeLineCTData(string key)
        {
            try
            {
                var keyArray = key.Split("_");

                var Id = keyArray[0];
                var line = keyArray[1];

                var ke = $"{Id}_{line}_SubVariant";

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
                Log.Error(ex, ex.ToString());
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
                Log.Error(ex, ex.ToString());
                return null;
            }

           

        }

        private async Task<LineWiseProdData?> ReadLineWiseProdData(string key)
        {
            try
            {
                var keyArray = key.Split('_');

                var lineId = keyArray[0];
                var hourId = keyArray[1];

                var nodesToRead = new ReadValueIdCollection
                {
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_Target"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_Actual"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_TargetJ5"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_ActualJ5"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_TargetV23"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_ActualV23"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_TimeStamp"]), AttributeId = Attributes.Value }
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

                return new LineWiseProdData
                {
                    LineID = Convert.ToInt32(lineId),
                    HourID = Convert.ToInt32(hourId),
                    Target = Convert.ToInt32(results.Results[0].Value),
                    Actual = Convert.ToInt32(results.Results[1].Value),
                    J5_Target = Convert.ToInt32(results.Results[2].Value),
                    J5_Actual = Convert.ToInt32(results.Results[3].Value),
                    V23_Target = Convert.ToInt32(results.Results[4].Value),
                    V23_Actual = Convert.ToInt32(results.Results[5].Value),
                    Timestamp = ConvertPlcDateTime((byte[])results.Results[6].Value),
                    LogDateTime = ConvertPlcDateTime((byte[])results.Results[6].Value),

                };

            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return null;
            }



        }


        private DateTime? ConvertPlcDateTime(byte[] bytes)
        {
            try
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
            catch(Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return null;
            }
            
        }

        private int BcdToInt(byte value)
        {
            return ((value >> 4) * 10) + (value & 0x0F);

        }
        #endregion

        #region DBInsertion       
        private async Task InsertAlarm(CancellationToken stoppingToken)
        {
            try
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
                        Log.Error(ex, "Error in background worker.");
                    }

                }
            }
            catch(Exception ex)
            {
                Log.Error(ex, ex.ToString());
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
                    Log.Error(ex, "Error in background worker.");
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

                    while (lineCTQueue.TryDequeue(out var CT))
                    {
                        batch.Add(CT);
                    }
                   
                    if (batch.Count == 0)
                    {
                        await Task.Delay(500);
                        continue;
                    }


                    if (batch.Count > 0)
                    {
                        string jsonString = JsonSerializer.Serialize(batch);
                        await bl.InsertLineCT(jsonString);
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error in background worker.");
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
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error in background worker.");
                }

            }
        }

        private async Task InsertLineWiseProdData(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _LineWiseProdDataSignal.WaitAsync(stoppingToken); 


                    List<LineWiseProdData> batch = new();

                   
                    while (lineWiseProdDataQueue.TryDequeue(out var SubSt))
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
                        await bl.InsertLineWiseProdData(jsonString);
                        //Console.WriteLine($"{batch[0].TagName} | {batch[0].Value} | {batch[0].Timestamp}");
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error in background worker.");
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
                    Log.Error("Error: PLC Disconnected: " + e.Status);

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

                            Log.Error("Error: trying to Reconnecting...");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }
        }

        private void Client_ReconnectComplete(object? sender, EventArgs e)
        {
            lock (lockObj)
            {
                try
                {
                    if (sender is SessionReconnectHandler handler)
                    {
                        session = (Session)handler.Session;
                        reconnectHandler = null;

                        Log.Information("PLC Reconnected Successfully!");
                    }
                }
                catch(Exception ex)
                {
                    Log.Error(ex, ex.ToString());
                }
                
            }
        }
        #endregion




    }
}

