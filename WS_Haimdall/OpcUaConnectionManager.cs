using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using Serilog;
using System.Net;

namespace WS_Haimdall
{
    

    public sealed class OpcUaConnectionManager : IDisposable
    {
        private readonly object _reconnectLock = new();
        private readonly SemaphoreSlim _connectionLock = new(1, 1);

        private Session? _session;
        private SessionReconnectHandler? _reconnectHandler;
        private ApplicationConfiguration? _appConfig;

        private bool _disposed;

        private const int ReconnectPeriod = 10_000;
        private const int SessionTimeout = 60_000;


        public Session? Session => _session;


        public bool IsConnected =>
            _session != null &&
            _session.Connected;


        public async Task<Session> ConnectAsync(
            string endpointUrl,
            string certSubjectName = "OpcUaClient",
            string? username = null,
            string? password = null)
        {
            await _connectionLock.WaitAsync();

            try
            {
                ThrowIfDisposed();

                if (_session != null && _session.Connected)
                {
                    Log.Information(
                        "OPC UA session already connected. SessionId: {SessionId}",
                        _session.SessionId);

                    return _session;
                }


                Log.Information(
                    "Starting OPC UA connection to {EndpointUrl}",
                    endpointUrl);


                if (_appConfig == null)
                {
                    _appConfig = await CreateApplicationConfigurationAsync(
                        certSubjectName);
                }


                EndpointDescription selectedEndpoint =
                    CoreClientUtils.SelectEndpoint(
                        _appConfig,
                        endpointUrl,
                        useSecurity: true);


                if (selectedEndpoint == null)
                {
                    throw new Exception(
                        "No secure OPC UA endpoint found.");
                }


                Log.Information(
                    "Selected OPC endpoint. URL: {EndpointUrl}, Mode: {SecurityMode}, Policy: {SecurityPolicy}",
                    selectedEndpoint.EndpointUrl,
                    selectedEndpoint.SecurityMode,
                    selectedEndpoint.SecurityPolicyUri);


                ValidateEndpointSecurity(selectedEndpoint);


                var configuredEndpoint = new ConfiguredEndpoint(
                    collection: null,
                    description: selectedEndpoint,
                    configuration: EndpointConfiguration.Create(_appConfig));


                IUserIdentity userIdentity =
                    CreateUserIdentity(
                        username,
                        password);


                Session newSession = await Session.Create(
                    configuration: _appConfig,
                    endpoint: configuredEndpoint,
                    updateBeforeConnect: true,
                    sessionName: "MyOpcSession",
                    sessionTimeout: SessionTimeout,
                    identity: userIdentity,
                    preferredLocales: null);


                if (newSession == null ||
                    !newSession.Connected)
                {
                    newSession?.Dispose();

                    throw new Exception(
                        "OPC UA session creation failed.");
                }


                CleanupSession();


                _session = newSession;


                _session.KeepAlive -= OpcSession_KeepAlive;
                _session.KeepAlive += OpcSession_KeepAlive;


                Log.Information(
                    "OPC UA connected successfully. SessionId: {SessionId}",
                    _session.SessionId);


                PrintConnectionInformation(
                    _session,
                    selectedEndpoint);


                return _session;
            }
            catch (ServiceResultException ex)
            {
                Log.Error(
                    ex,
                    "OPC UA ServiceResultException while connecting");

                throw;
            }
            catch (TimeoutException ex)
            {
                Log.Error(
                    ex,
                    "OPC UA connection timeout");

                throw;
            }
            catch (Exception ex)
            {
                Log.Error(
                    ex,
                    "Error while connecting OPC UA session");

                throw;
            }
            finally
            {
                _connectionLock.Release();
            }
        }


        private async Task<ApplicationConfiguration>
            CreateApplicationConfigurationAsync(
                string certSubjectName)
        {
            string hostName = Dns.GetHostName();


            Log.Information(
                "Creating OPC UA application configuration");


            var appConfig = new ApplicationConfiguration
            {
                ApplicationName = "OpcUaClientApp",

                ApplicationUri =
                    $"urn:{hostName}:OpcUaClientApp",

                ApplicationType =
                    ApplicationType.Client,


                SecurityConfiguration =
                    new SecurityConfiguration
                    {
                        ApplicationCertificate =
                            new CertificateIdentifier
                            {
                                StoreType = @"Directory",

                                StorePath =
                                    @"%LocalAppData%/OPC/Certificates/own",

                                SubjectName =
                                    $"CN={certSubjectName}, DC={hostName}"
                            },


                        TrustedIssuerCertificates =
                            new CertificateTrustList
                            {
                                StoreType = @"Directory",

                                StorePath =
                                    @"%LocalAppData%/OPC/Certificates/issuers"
                            },


                        TrustedPeerCertificates =
                            new CertificateTrustList
                            {
                                StoreType = @"Directory",

                                StorePath =
                                    @"%LocalAppData%/OPC/Certificates/trusted"
                            },


                        RejectedCertificateStore =
                            new CertificateTrustList
                            {
                                StoreType = @"Directory",

                                StorePath =
                                    @"%LocalAppData%/OPC/Certificates/rejected"
                            },


                        AutoAcceptUntrustedCertificates = true,

                        AddAppCertToTrustedStore = true,

                        MinimumCertificateKeySize = 2048
                    },


                TransportConfigurations =
                    new TransportConfigurationCollection(),


                TransportQuotas =
                    new TransportQuotas
                    {
                        OperationTimeout = 15_000,

                        MaxStringLength = 1_048_576,

                        MaxByteStringLength = 1_048_576,

                        MaxArrayLength = 65_535,

                        MaxMessageSize = 4_194_304
                    },


                ClientConfiguration =
                    new ClientConfiguration
                    {
                        DefaultSessionTimeout =
                            SessionTimeout,

                        MinSubscriptionLifetime =
                            10_000
                    }
            };


            await appConfig.Validate(
                ApplicationType.Client);


            var appInstance =
                new ApplicationInstance
                {
                    ApplicationName =
                        appConfig.ApplicationName,

                    ApplicationType =
                        appConfig.ApplicationType,

                    ApplicationConfiguration =
                        appConfig
                };


            bool certificateOk =
                await appInstance
                    .CheckApplicationInstanceCertificatesAsync(
                        false);


            if (!certificateOk)
            {
                throw new Exception(
                    "OPC UA application certificate check failed.");
            }


            Log.Information(
                "OPC UA application configuration created successfully");


            return appConfig;
        }


        private static IUserIdentity CreateUserIdentity(
            string? username,
            string? password)
        {
            if (!string.IsNullOrWhiteSpace(username))
            {
                return new UserIdentity(
                    username,
                    Encoding.UTF8.GetBytes(password ?? string.Empty));
            }


            return new UserIdentity(
                new AnonymousIdentityToken());
        }


        private static void ValidateEndpointSecurity(
            EndpointDescription selectedEndpoint)
        {
            if (selectedEndpoint.SecurityMode !=
                MessageSecurityMode.SignAndEncrypt)
            {
                throw new ServiceResultException(
                    StatusCodes.BadSecurityModeRejected,
                    $"Expected SignAndEncrypt but server selected {selectedEndpoint.SecurityMode}");
            }


            if (selectedEndpoint.SecurityPolicyUri !=
                SecurityPolicies.Aes256_Sha256_RsaPss)
            {
                throw new ServiceResultException(
                    StatusCodes.BadSecurityPolicyRejected,
                    $"Expected {SecurityPolicies.Aes256_Sha256_RsaPss} but server selected {selectedEndpoint.SecurityPolicyUri}");
            }
        }


        private void OpcSession_KeepAlive(
            ISession senderSession,
            KeepAliveEventArgs e)
        {
            try
            {
                if (e.Status == null ||
                    !ServiceResult.IsNotGood(e.Status))
                {
                    return;
                }


                lock (_reconnectLock)
                {
                    if (_disposed)
                    {
                        return;
                    }


                    if (_reconnectHandler != null)
                    {
                        return;
                    }


                    Log.Warning(
                        "PLC disconnected. OPC Status: {Status}. Starting reconnect.",
                        e.Status);


                    _reconnectHandler =
                        new SessionReconnectHandler();


                    _reconnectHandler.BeginReconnect(
                        senderSession,
                        ReconnectPeriod,
                        Client_ReconnectComplete);
                }
            }
            catch (Exception ex)
            {
                Log.Error(
                    ex,
                    "Error in OPC UA KeepAlive handler");
            }
        }


        private void Client_ReconnectComplete(
            object? sender,
            EventArgs e)
        {
            lock (_reconnectLock)
            {
                SessionReconnectHandler? completedHandler = null;

                try
                {
                    if (_disposed)
                    {
                        return;
                    }


                    if (sender is not SessionReconnectHandler handler)
                    {
                        return;
                    }


                    if (!ReferenceEquals(
                            _reconnectHandler,
                            handler))
                    {
                        return;
                    }


                    completedHandler = handler;


                    Session? reconnectedSession =
                        handler.Session as Session;


                    if (reconnectedSession == null)
                    {
                        throw new Exception(
                            "Reconnect completed but OPC UA session is null.");
                    }


                    Session? oldSession = _session;


                    if (!ReferenceEquals(
                            oldSession,
                            reconnectedSession))
                    {
                        if (oldSession != null)
                        {
                            oldSession.KeepAlive -=
                                OpcSession_KeepAlive;
                        }


                        reconnectedSession.KeepAlive -=
                            OpcSession_KeepAlive;

                        reconnectedSession.KeepAlive +=
                            OpcSession_KeepAlive;
                    }


                    _session = reconnectedSession;


                    _reconnectHandler = null;


                    Log.Information(
                        "PLC reconnected successfully. SessionId: {SessionId}, Subscriptions: {SubscriptionCount}",
                        _session.SessionId,
                        _session.SubscriptionCount);


                    Console.WriteLine(
                        "─────────────────────────────────────");

                    Console.WriteLine(
                        "       OPC UA Reconnected");

                    Console.WriteLine(
                        "─────────────────────────────────────");

                    Console.WriteLine(
                        $" Session ID     : {_session.SessionId}");

                    Console.WriteLine(
                        $" Subscription   : {_session.SubscriptionCount}");

                    Console.WriteLine(
                        "─────────────────────────────────────");
                }
                catch (Exception ex)
                {
                    Log.Error(
                        ex,
                        "Error while completing OPC UA reconnect");


                    _reconnectHandler = null;
                }
                finally
                {
                    try
                    {
                        completedHandler?.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Log.Warning(
                            ex,
                            "Error while disposing OPC UA reconnect handler");
                    }
                }
            }
        }


        private void CleanupSession()
        {
            Session? oldSession = _session;


            _session = null;


            if (oldSession == null)
            {
                return;
            }


            try
            {
                oldSession.KeepAlive -=
                    OpcSession_KeepAlive;


                if (oldSession.Connected)
                {
                    oldSession.Close();
                }


                oldSession.Dispose();


                Log.Information(
                    "Old OPC UA session disposed successfully");
            }
            catch (Exception ex)
            {
                Log.Warning(
                    ex,
                    "Error while disposing OPC UA session");
            }
        }


        private void CleanupReconnectHandler()
        {
            lock (_reconnectLock)
            {
                if (_reconnectHandler == null)
                {
                    return;
                }


                try
                {
                    _reconnectHandler.Dispose();


                    Log.Information(
                        "OPC UA reconnect handler disposed");
                }
                catch (Exception ex)
                {
                    Log.Warning(
                        ex,
                        "Error while disposing OPC UA reconnect handler");
                }
                finally
                {
                    _reconnectHandler = null;
                }
            }
        }


        private static void PrintConnectionInformation(
            Session session,
            EndpointDescription selectedEndpoint)
        {
            Console.WriteLine(
                "─────────────────────────────────────");

            Console.WriteLine(
                "        OPC UA Connected");

            Console.WriteLine(
                "─────────────────────────────────────");

            Console.WriteLine(
                $" Session ID      : {session.SessionId}");

            Console.WriteLine(
                $" Server URI      : {session.Endpoint.Server.ApplicationUri}");

            Console.WriteLine(
                $" Endpoint URL    : {session.Endpoint.EndpointUrl}");

            Console.WriteLine(
                $" Security Mode   : {session.Endpoint.SecurityMode}");

            Console.WriteLine(
                $" Security Policy : {session.Endpoint.SecurityPolicyUri}");

            Console.WriteLine(
                $" Timeout (ms)    : {session.SessionTimeout}");

            Console.WriteLine(
                $" Subscriptions   : {session.SubscriptionCount}");

            Console.WriteLine(
                "─────────────────────────────────────");
        }


        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(
                    nameof(OpcUaConnectionManager));
            }
        }


        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }


            _disposed = true;


            Log.Information(
                "Disposing OPC UA Connection Manager");


            CleanupReconnectHandler();


            CleanupSession();


            _connectionLock.Dispose();


            GC.SuppressFinalize(this);
        }
    }
}
