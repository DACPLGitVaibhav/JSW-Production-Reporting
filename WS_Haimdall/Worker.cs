
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

        /// <summary>
        /// for Losses subscriptions
        /// </summary>
        private Subscription _LossesSubscription;
        private ConcurrentDictionary<string, string> LossesCache = new();
        private static readonly ConcurrentQueue<LossesData> LossesQueue = new();
        private readonly SemaphoreSlim _LossesSignal = new(0);

        /// <summary>
        /// for OEE subscriptions
        /// </summary>
        private Subscription _OeeSubscription;
        private ConcurrentDictionary<string, string> OeeCache = new();
        private static readonly ConcurrentQueue<OeeData> OeeQueue = new();
        private readonly SemaphoreSlim _OeeSignal = new(0);

        /// <summary>
        /// for MTTR and MTBF subscriptions
        /// </summary>
        private Subscription _MTTR_MTBF_Subscription;
        private ConcurrentDictionary<string, string> MTTR_MTBF_Cache = new();
        private static readonly ConcurrentQueue<MTTR_MTBF_Data> MTTR_MTBF_Queue = new();
        private readonly SemaphoreSlim _MTTR_MTBF_Signal = new(0);


        private static Dictionary<string, string> tagDict;
        #endregion

        public Worker(ILogger<Worker> logger, IOptions<appSettings> options)
        {
           // _logger = logger;
            _settings = options.Value;
            bl = new BusinessLayer(_settings.DB_Connection, _settings.PlcNo);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                Log.Information("Success: Service Started.");

                bl.FillAlarmMaster();
                bl.FillNodeIdConfig();

                await ConnectOPCSession();


                Log.Information("Ready/Started to Insert data..");
                var alarmTask = InsertAlarm(stoppingToken);
                var ctTask = InsertCT(stoppingToken);
                var lineCtTask = InsertLineCT(stoppingToken);
                var subStTask = InsertSubStationCT(stoppingToken);
                var lineWiseProdData = InsertLineWiseProdData(stoppingToken);
                var lossesData = InsertLossesData(stoppingToken);
                var oeeData = InsertOeeData(stoppingToken);
                var mttr_mtbfData = InsertMTTR_MTBF_Data(stoppingToken);

                await Task.WhenAll(alarmTask, ctTask, lineCtTask, subStTask, lineWiseProdData, lossesData, oeeData, mttr_mtbfData);
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
                var endpointUrl = "opc.tcp://192.168.0.13:4840"; //"opc.tcp://192.168.196.1:4840" Replace with your server's endpoint URL //_settings.Endpoint

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
                    Log.Information("Subscribing tags..");


                    //Alarm 
                    //CreateAlarmSubscription();
                    
                    if(_settings.isLastPlc)
                    {
                        //Line CT
                        await CreateCycleTime_LineCTSubscription();

                        //Prod
                        await CreateLineWise_ProdDataSubscription();
                    }
                        
                    //SubStation CT
                    await CreateCycleTime_SubstationCTSubscription();

                    //Losses
                    await CreateLosses_Subscription();

                    //OEE
                    await CreateOEE_Subscription();

                    //MTTR and MTBF
                    await CreateMTTR_MTBF_Subscription();

                    Log.Information("Subscribed necessary tags.");
                    #endregion
                }
                

            }
            catch (ServiceResultException ex)
            {
                Log.Error(ex, "Error at ServiceResultException" + ex.ToString());
            }

            catch (TimeoutException ex)
            {
                Log.Error(ex, "Error at TimeoutException: " + ex.ToString());
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error at ConnectOPCSession" + ex.ToString());

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
                    if (!eachItem.Key.Contains("_Biwno"))
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
                    if (!eachItem.Key.Contains("_Biwno"))
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
                    if (!eachItem.Key.EndsWith("_HourlyActual"))
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


        private async Task CreateLosses_Subscription()
        {
            try
            {
                _LossesSubscription = new Subscription(session.DefaultSubscription)
                {
                    PublishingInterval = 250,
                    DisplayName = "CycleTriggerSubscription",
                    PublishingEnabled = true
                };

                session.AddSubscription(_LossesSubscription);
                await _LossesSubscription.CreateAsync();

                var monitoredItems = new List<Opc.Ua.Client.MonitoredItem>();

                foreach (var eachItem in dict_NodeIdConfigLosses)
                {
                    if (!eachItem.Key.Contains("_Total"))
                        continue;

                    string nodeIdStr = eachItem.Value;
                    var item = new Opc.Ua.Client.MonitoredItem(_LossesSubscription.DefaultItem)
                    {
                        DisplayName = eachItem.Key.ToString(),// nodeIdStr,
                        StartNodeId = new NodeId(nodeIdStr),
                        AttributeId = Attributes.Value,
                        SamplingInterval = 500,
                        QueueSize = 10,
                        DiscardOldest = true
                    };

                    // Correct way to attach the notification handler
                    item.Notification += OnLossesTrigger;

                    monitoredItems.Add(item);
                }


                _LossesSubscription.AddItems(monitoredItems);

                await _LossesSubscription.ApplyChangesAsync();
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }

        }

        private async Task CreateOEE_Subscription()
        {
            try
            {
                _OeeSubscription = new Subscription(session.DefaultSubscription)
                {
                    PublishingInterval = 250,
                    DisplayName = "CycleTriggerSubscription",
                    PublishingEnabled = true
                };

                session.AddSubscription(_OeeSubscription);
                await _OeeSubscription.CreateAsync();

                var monitoredItems = new List<Opc.Ua.Client.MonitoredItem>();

                foreach (var eachItem in dict_NodeIdConfigOee)
                {
                    if (!eachItem.Key.Contains("_OEE_"))
                        continue;

                    string nodeIdStr = eachItem.Value;
                    var item = new Opc.Ua.Client.MonitoredItem(_OeeSubscription.DefaultItem)
                    {
                        DisplayName = eachItem.Key.ToString(),// nodeIdStr,
                        StartNodeId = new NodeId(nodeIdStr),
                        AttributeId = Attributes.Value,
                        SamplingInterval = 500,
                        QueueSize = 10,
                        DiscardOldest = true
                    };

                    // Correct way to attach the notification handler
                    item.Notification += OnOeeTrigger;

                    monitoredItems.Add(item);
                }


                _OeeSubscription.AddItems(monitoredItems);

                await _OeeSubscription.ApplyChangesAsync();
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }

        }

        private async Task CreateMTTR_MTBF_Subscription()
        {
            try
            {
                _MTTR_MTBF_Subscription = new Subscription(session.DefaultSubscription)
                {
                    PublishingInterval = 250,
                    DisplayName = "CycleTriggerSubscription",
                    PublishingEnabled = true
                };

                session.AddSubscription(_MTTR_MTBF_Subscription);
                await _MTTR_MTBF_Subscription.CreateAsync();

                var monitoredItems = new List<Opc.Ua.Client.MonitoredItem>();

                foreach (var eachItem in dict_NodeIdConfigMTTRMTBF)
                {
                    if (!eachItem.Key.Contains("_MTTR_"))
                        continue;

                    string nodeIdStr = eachItem.Value;
                    var item = new Opc.Ua.Client.MonitoredItem(_MTTR_MTBF_Subscription.DefaultItem)
                    {
                        DisplayName = eachItem.Key.ToString(),// nodeIdStr,
                        StartNodeId = new NodeId(nodeIdStr),
                        AttributeId = Attributes.Value,
                        SamplingInterval = 500,
                        QueueSize = 10,
                        DiscardOldest = true
                    };

                    // Correct way to attach the notification handler
                    item.Notification += OnMTTR_MTBF_Trigger;

                    monitoredItems.Add(item);
                }


                _MTTR_MTBF_Subscription.AddItems(monitoredItems);

                await _MTTR_MTBF_Subscription.ApplyChangesAsync();
            }
            catch (Exception ex)
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

                    var sourceTime = value.SourceTimestamp.ToLocalTime();

                    // SQL-friendly format
                    var sqlTimeStamp = sourceTime.ToString("yyyy-MM-dd HH:mm:ss.fff");

                    var timeStamp = Convert.ToDateTime(sqlTimeStamp);

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
                        var cycleTimeLineCTData = await ReadCycleTimeLineCTData(tag, timeStamp);
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

                    var sourceTime = value.SourceTimestamp.ToLocalTime();

                    // SQL-friendly format
                    var sqlTimeStamp = sourceTime.ToString("yyyy-MM-dd HH:mm:ss.fff");

                    var timeStamp = Convert.ToDateTime(sqlTimeStamp);

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
                        var cycleTimeSubStationCTData = await ReadCycleTimeSubStationCTData(tag, timeStamp);
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
                   // var timeStamp = value.SourceTimestamp.ToUniversalTime();

                    var sourceTime = value.SourceTimestamp.ToLocalTime();

                    // SQL-friendly format
                    var sqlTimeStamp = sourceTime.ToString("yyyy-MM-dd HH:mm:ss.fff");

                    var timeStamp = Convert.ToDateTime(sqlTimeStamp);

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
                        var lineWiseProdData = await ReadLineWiseProdData(tag, timeStamp);
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


        private async void OnLossesTrigger(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            try
            {
                foreach (var value in item.DequeueValues())
                {
                    string tag = item.DisplayName;

                    var sourceTime = value.SourceTimestamp.ToLocalTime();

                    // SQL-friendly format
                    var sqlTimeStamp = sourceTime.ToString("yyyy-MM-dd HH:mm:ss.fff");

                    var timeStamp = Convert.ToDateTime(sqlTimeStamp);

                    var itemId = value.Value?.ToString();

                    if (string.IsNullOrEmpty(itemId) || itemId == "0")
                        return;



                    // 🔥 Check duplicate
                    if (LossesCache.TryGetValue(tag, out var lastVal))
                    {
                        if (lastVal == itemId)
                            continue; // ❌ skip duplicate
                    }

                    // ✅ update cache
                    LossesCache[tag] = itemId;
                    //////////////////////////
                    ///
                    try
                    {
                        var losssesData = await ReadLossesData(tag, timeStamp);
                        if (losssesData != null)
                        {
                            LossesQueue.Enqueue(losssesData);
                            _LossesSignal.Release();
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "Error reading cycle data");
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }

        }

        private async void OnOeeTrigger(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            try
            {
                foreach (var value in item.DequeueValues())
                {
                    string tag = item.DisplayName;

                    var sourceTime = value.SourceTimestamp.ToLocalTime();

                    // SQL-friendly format
                    var sqlTimeStamp = sourceTime.ToString("yyyy-MM-dd HH:mm:ss.fff");

                    var timeStamp = Convert.ToDateTime(sqlTimeStamp);

                    var itemId = value.Value?.ToString();

                    if (string.IsNullOrEmpty(itemId) || itemId == "0")
                        return;



                    // 🔥 Check duplicate
                    if (OeeCache.TryGetValue(tag, out var lastVal))
                    {
                        if (lastVal == itemId)
                            continue; // ❌ skip duplicate
                    }

                    // ✅ update cache
                    OeeCache[tag] = itemId;
                    //////////////////////////
                    ///
                    try
                    {
                        var OeeData = await ReadOeeData(tag, timeStamp);
                        if (OeeData != null)
                        {
                            OeeQueue.Enqueue(OeeData);
                            _OeeSignal.Release();
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "Error reading cycle data");
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }

        }

        private async void OnMTTR_MTBF_Trigger(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            try
            {
                foreach (var value in item.DequeueValues())
                {
                    string tag = item.DisplayName;

                    var sourceTime = value.SourceTimestamp.ToLocalTime();

                    // SQL-friendly format
                    var sqlTimeStamp = sourceTime.ToString("yyyy-MM-dd HH:mm:ss.fff");

                    var timeStamp = Convert.ToDateTime(sqlTimeStamp);

                    var itemId = value.Value?.ToString();

                    if (string.IsNullOrEmpty(itemId) || itemId == "0")
                        return;



                    // 🔥 Check duplicate
                    if (MTTR_MTBF_Cache.TryGetValue(tag, out var lastVal))
                    {
                        if (lastVal == itemId)
                            continue; // ❌ skip duplicate
                    }

                    // ✅ update cache
                    MTTR_MTBF_Cache[tag] = itemId;
                    //////////////////////////
                    ///
                    try
                    {
                        var MttrMtbfData = await ReadMTTRMTBFData(tag, timeStamp);
                        if (MttrMtbfData != null)
                        {
                            MTTR_MTBF_Queue.Enqueue(MttrMtbfData);
                            _MTTR_MTBF_Signal.Release();
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "Error reading cycle data");
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
            }

        }
        #endregion

        #region DataRead       

        private async Task<CycleTimeLineCTData?> ReadCycleTimeLineCTData(string key, DateTime _timeStamp)
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
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineCT[$"{Id}_{line}_Biwno"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineCT[$"{Id}_{line}_VariantCode"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineCT[$"{Id}_{line}_SubVariantCode"]), AttributeId = Attributes.Value }
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
                    TimeStamp = _timeStamp,//ConvertPlcDateTime((byte[])results.Results[1].Value),
                };
            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return null;
            }
            
        }

        private async Task<CycleTimeSubStaionCTData?> ReadCycleTimeSubStationCTData(string key, DateTime _timeStamp)
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
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_Biwno"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_VariantCode"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_SubVariantCode"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_Emergency"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_TipChange"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_TipDress"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_OperatorLoadingStayingTime"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigSubstationCT[$"{Id}_{subStation}_BlockTime"]), AttributeId = Attributes.Value },
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
                    TimeStamp = _timeStamp,//ConvertPlcDateTime((byte[])results.Results[1].Value),
                    Sub_StationID = Convert.ToInt32(Id)

                };

            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return null;
            }

           

        }

        private async Task<LineWiseProdData?> ReadLineWiseProdData(string key, DateTime _timeStamp)
        {
            try
            {
                var keyArray = key.Split('_');

                var lineId = keyArray[0];
                var hourId = keyArray[1];

                var nodesToRead = new ReadValueIdCollection
                {
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_ShiftTarget"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_JPHJ5V23"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_HourlyActual"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_J5Actual"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLineWiseProdData[$"{lineId}_{hourId}_V23Actual"]), AttributeId = Attributes.Value }
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
                    JPH_J5_V23 = Convert.ToInt32(results.Results[1].Value),
                    Actual = Convert.ToInt32(results.Results[2].Value),
                    J5_Target = 0,
                    J5_Actual = Convert.ToInt32(results.Results[3].Value),
                    V23_Target = 0,
                    V23_Actual = Convert.ToInt32(results.Results[4].Value),
                    
                    Timestamp = _timeStamp,
                    LogDateTime = _timeStamp,

                };

            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return null;
            }



        }

        private async Task<LossesData?> ReadLossesData(string key, DateTime _timeStamp)
        {
            try
            {
                var keyArray = key.Split('_');

                var Id = keyArray[0];
                var subStation = keyArray[1];
                var shift = keyArray[3];

                var nodesToRead = new ReadValueIdCollection
                {
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_Emergency_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_TipChange_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_TipDress_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_OperatorLoadingStarvingTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_BlockTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_Manual_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_PartPresentFault_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_RollMoveTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_LifterMoveTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_TurnTableMoveTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_ClampTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_DeclampTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_MarriageMissMatch_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_DropTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_WeldTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_PickTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_SealingTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_SafetyGate_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_Miscellaneous_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_MaterialCall_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_Others_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigLosses[$"{Id}_{subStation}_Total_{shift}"]), AttributeId = Attributes.Value }
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

                return new LossesData
                {
                    SubStationID = Convert.ToInt32(Id),
                    Shift = shift,

                    Emergency = Convert.ToInt32(results.Results[0].Value),
                    Tip_Change = Convert.ToInt32(results.Results[1].Value),
                    Tip_Dress = Convert.ToInt32(results.Results[2].Value),
                    OperatorLoading_Starving_Time = Convert.ToInt32(results.Results[3].Value),
                    Block_Time = Convert.ToInt32(results.Results[4].Value),
                    Manual = Convert.ToInt32(results.Results[5].Value),
                    Part_Present_Fault = Convert.ToInt32(results.Results[6].Value),
                    RollMoveTime = Convert.ToInt32(results.Results[7].Value),
                    LifterMoveTime = Convert.ToInt32(results.Results[8].Value),
                    TurnTableMoveTime = Convert.ToInt32(results.Results[9].Value),
                    ClampTime = Convert.ToInt32(results.Results[10].Value),
                    DeclampTime = Convert.ToInt32(results.Results[11].Value),
                    Marriage_Miss_Match = Convert.ToInt32(results.Results[12].Value),
                    DropTime = Convert.ToInt32(results.Results[13].Value),
                    WeldTime = Convert.ToInt32(results.Results[14].Value),
                    PickTime = Convert.ToInt32(results.Results[15].Value),
                    SealingTime = Convert.ToInt32(results.Results[16].Value),
                    Safety_Gate = Convert.ToInt32(results.Results[17].Value),
                    Miscellaneous = Convert.ToInt32(results.Results[18].Value),
                    MaterialCall = Convert.ToInt32(results.Results[19].Value),
                    Others = Convert.ToInt32(results.Results[20].Value),
                    Total = Convert.ToInt32(results.Results[21].Value),

                    NoOfOccurance = 0,
                    Timestamp = _timeStamp
                };

            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return null;
            }



        }

        private async Task<OeeData?> ReadOeeData(string key, DateTime _timeStamp)
        {
            try
            {
                var keyArray = key.Split('_');

                var Id = keyArray[0];
                var subStation = keyArray[1];
                var shift = keyArray[3];

                var nodesToRead = new ReadValueIdCollection
                {
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigOee[$"{Id}_{subStation}_Availability_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigOee[$"{Id}_{subStation}_Performance_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigOee[$"{Id}_{subStation}_Quality_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigOee[$"{Id}_{subStation}_OEE_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigOee[$"{Id}_{subStation}_NetAvailOperTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigOee[$"{Id}_{subStation}_BreakDownTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigOee[$"{Id}_{subStation}_PerformanceLossTime_{shift}"]), AttributeId = Attributes.Value }
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

                return new OeeData
                {
                    SubStationID = Convert.ToInt32(Id),
                    Shift = shift,

                    Availability = Convert.ToSingle(results.Results[0].Value),
                    Performance = Convert.ToSingle(results.Results[1].Value),
                    Quality = Convert.ToSingle(results.Results[2].Value),
                    OEE = Convert.ToSingle(results.Results[3].Value),

                    NetAvail_OperTime = Convert.ToInt32(results.Results[4].Value),
                    BreakDownTime = Convert.ToInt32(results.Results[5].Value),
                    PerformanceLossTime = Convert.ToInt32(results.Results[6].Value),

                    Timestamp = _timeStamp
                };

            }
            catch (Exception ex)
            {
                Log.Error(ex, ex.ToString());
                return null;
            }



        }

        private async Task<MTTR_MTBF_Data?> ReadMTTRMTBFData(string key, DateTime _timeStamp)
        {
            try
            {
                var keyArray = key.Split('_');

                var Id = keyArray[0];
                var subStation = keyArray[1];
                var shift = keyArray[3];

                var nodesToRead = new ReadValueIdCollection
                {
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigMTTRMTBF[$"{Id}_{subStation}_MTTR_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigMTTRMTBF[$"{Id}_{subStation}_MTBF_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigMTTRMTBF[$"{Id}_{subStation}_NoOfFailure_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigMTTRMTBF[$"{Id}_{subStation}_NetAvailOperTime_{shift}"]), AttributeId = Attributes.Value },
                    new ReadValueId { NodeId = NodeId.Parse(dict_NodeIdConfigMTTRMTBF[$"{Id}_{subStation}_BreakDownTime_{shift}"]), AttributeId = Attributes.Value }
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

                return new MTTR_MTBF_Data
                {
                    SubStationID = Convert.ToInt32(Id),
                    Shift = shift,

                    MTTR = Convert.ToSingle(results.Results[0].Value),
                    MTBF = Convert.ToSingle(results.Results[1].Value),

                    NoOfFailure = Convert.ToInt32(results.Results[2].Value),
                    NetAvail_OperTime = Convert.ToInt32(results.Results[3].Value),
                    BreakDownTime = Convert.ToInt32(results.Results[4].Value),

                    Timestamp = _timeStamp
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


        private async Task InsertLossesData(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _LossesSignal.WaitAsync(stoppingToken);


                    List<LossesData> batch = new();


                    while (LossesQueue.TryDequeue(out var SubSt))
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
                        await bl.InsertLossesData(jsonString);
                        //Console.WriteLine($"{batch[0].TagName} | {batch[0].Value} | {batch[0].Timestamp}");
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error in background worker.");
                }

            }
        }

        private async Task InsertOeeData(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _OeeSignal.WaitAsync(stoppingToken);


                    List<OeeData> batch = new();


                    while (OeeQueue.TryDequeue(out var SubSt))
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
                        await bl.InsertOeeData(jsonString);
                        //Console.WriteLine($"{batch[0].TagName} | {batch[0].Value} | {batch[0].Timestamp}");
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error in background worker.");
                }

            }
        }

        private async Task InsertMTTR_MTBF_Data(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _MTTR_MTBF_Signal.WaitAsync(stoppingToken);


                    List<MTTR_MTBF_Data> batch = new();


                    while (MTTR_MTBF_Queue.TryDequeue(out var SubSt))
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
                        await bl.InsertMTTR_MTBFData(jsonString);
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

