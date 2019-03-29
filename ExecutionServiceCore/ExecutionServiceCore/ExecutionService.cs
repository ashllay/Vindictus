using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using Devcat.Core.Threading;
using ExecutionServiceCore.Properties;
using ServiceCore;
using ServiceCore.Configuration;
using ServiceCore.ExecutionServiceOperations;
using UnifiedNetwork.Cooperation;
using UnifiedNetwork.OperationService;
using Utility;

namespace ExecutionServiceCore
{
    public class ExecutionService : Service
    {
        private static IEnumerable<AppDomain> EnumDomains()
        {
            return CLRUtil.EnumAppDomains();
        }

        public ExecutionService(string addr, string port)
        {
            this.parameters = new object[]
            {
                addr,
                port
            };
            this.serviceAvailability = ExecutionService.Domain.Do(this.parameters);
        }

        private Configuration Configuration
        {
            get
            {
                Configuration result;
                try
                {
                    Assembly assembly = typeof(ExecutionService).Assembly;
                    Configuration configuration = ConfigurationManager.OpenExeConfiguration(assembly.Location);
                    result = configuration;
                }
                catch (ConfigurationErrorsException)
                {
                    result = null;
                }
                return result;
            }
        }

        public override void Initialize(JobProcessor thread)
        {
            ConnectionStringLoader.LoadFromServiceCore(Settings.Default);
            base.Initialize(thread, ExecutionServiceOperations.TypeConverters);
            base.RegisterMessage(OperationMessages.TypeConverters);
            base.RegisterProcessor(typeof(StartService), (Operation op) => new StartServiceProcessor(this, op as StartService));
            base.RegisterProcessor(typeof(QueryService), (Operation op) => new QueryServiceProcessor(this, op as QueryService));
            base.RegisterProcessor(typeof(ExecAppDomain), (Operation op) => new ExecAppDomainProcessor(this, op as ExecAppDomain));
            LocalServiceConfig localServiceConfig = this.Configuration.GetSection("LocalServiceConfig") as LocalServiceConfig;
            foreach (object obj in localServiceConfig.ServiceInfos)
            {
                LocalServiceConfig.Service service = (LocalServiceConfig.Service)obj;
                if (service.AutoStart)
                {
                    Scheduler.Schedule(base.Thread, Job.Create<string>(delegate (string serviceClass)
                    {
                        Log<ExecutionService>.Logger.DebugFormat("<Initialize : [{0}]> auto start...", serviceClass);
                        string text;
                        this.StartAppDomain(serviceClass, out text);
                        Log<ExecutionService>.Logger.DebugFormat("<Initialize : [{0}]> auto start invoked : {1}", serviceClass, text);
                    }, service.ServiceClass), 0);
                }
            }
            base.Disposed += delegate (object sender, EventArgs e)
            {
                foreach (string domainName in new List<string>(this.DomainList))
                {
                    this.StopAppDomain(domainName);
                }
            };
        }

        public IEnumerable<string> Services
        {
            get
            {
                return this.serviceAvailability.Keys;
            }
        }

        public IEnumerable<string> DomainList
        {
            get
            {
                return from domain in ExecutionService.EnumDomains()
                       where this.appDomains.ContainsKey(domain.FriendlyName)
                       select domain.FriendlyName;
            }
        }

        public bool StartService(string serviceClass, out string message)
        {
            return this.StartAppDomain(serviceClass, out message);
        }

        public bool StartAppDomain(string serviceClass, out string message)
        {
            message = "";
            Invoker invoker;
            if (!this.serviceAvailability.TryGetValue(serviceClass, out invoker))
            {
                Log<ExecutionService>.Logger.DebugFormat("<StartAppDomain> [{0}] 도메인은 시작 대상이 아닙니다.", serviceClass);
                return false;
            }
            int num = 0;
            string text = serviceClass;
            while (this.appDomains.ContainsKey(text))
            {
                text = string.Format("{0}[{1}]", serviceClass, ++num);
            }
            Log<ExecutionService>.Logger.DebugFormat("<StartAppDomain> [{0}] 도메인 시작", serviceClass);
            invoker.DomainName = text;
            try
            {
                AppDomain appDomain = invoker.Execute();
                this.appDomains.Add(appDomain.FriendlyName, appDomain);
            }
            catch (Exception ex)
            {
                Log<ExecutionService>.Logger.Fatal(string.Format("StartAppDomain[serviceClass = {0}]", serviceClass), ex);
                return false;
            }
            message = serviceClass;
            return true;
        }

        public bool StopAppDomain(string domainName)
        {
            Log<ExecutionService>.Logger.DebugFormat("<StopAppDomain> [{0}] 도메인 중지", domainName);
            AppDomain domain;
            if (this.appDomains.TryGetValue(domainName, out domain))
            {
                try
                {
                    AppDomain.Unload(domain);
                }
                catch (CannotUnloadAppDomainException ex)
                {
                    Log<ExecutionService>.Logger.Error(string.Format("StopAppDomain[domainName = {0}]", domainName), ex);
                    return false;
                }
                this.appDomains.Remove(domainName);
                return true;
            }
            Log<ExecutionService>.Logger.DebugFormat("<StopAppDomain> [{0}] 도메인은 중지 대상이 아닙니다.", domainName);
            return false;
        }

        public static void StartService(string ip, string portstr)
        {
            ServiceInvoker.StartService(ip, portstr, new ExecutionService(ip, portstr));
        }

        private object[] parameters;

        private IDictionary<string, Invoker> serviceAvailability = new Dictionary<string, Invoker>();

        private IDictionary<string, AppDomain> appDomains = new Dictionary<string, AppDomain>();

        private class Domain : IDisposable
        {
            private Domain()
            {
                AppDomainSetup info = new AppDomainSetup
                {
                    ApplicationName = typeof(ExecutionService.Domain).Name,
                    PrivateBinPath = ExecutionService.Domain.privateBinPath
                };
                this.domain = AppDomain.CreateDomain(typeof(ExecutionService.Domain).FullName, AppDomain.CurrentDomain.Evidence, info);
                this.domain.UnhandledException += ExecutionService.Domain.domain_UnhandledException;
            }

            public static IDictionary<string, Invoker> Do(object[] parameters)
            {
                IDictionary<string, Invoker> result;
                using (ExecutionService.Domain domain = new ExecutionService.Domain())
                {
                    domain.domain.SetData("param", parameters);
                    domain.domain.DoCallBack(new CrossAppDomainDelegate(ExecutionService.Domain.CallBack));
                    result = (domain.domain.GetData("returns") as IDictionary<string, Invoker>);
                }
                return result;
            }

            private static void domain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
            {
                Log<ExecutionService.Domain>.Logger.Fatal(sender, e.ExceptionObject as Exception);
            }

            private static void CallBack()
            {
                object[] parameters = AppDomain.CurrentDomain.GetData("param") as object[];
                Dictionary<string, Invoker> dictionary = new Dictionary<string, Invoker>();
                foreach (string assemblyFile in Directory.GetFiles(Environment.CurrentDirectory, "*.dll"))
                {
                    try
                    {
                        Assembly assembly = Assembly.LoadFrom(assemblyFile);
                        foreach (Type type in assembly.GetTypes())
                        {
                            try
                            {
                                MethodInfo method = type.GetMethod("StartService", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic, null, CallingConventions.Standard, new Type[]
                                {
                                    typeof(string),
                                    typeof(string)
                                }, null);
                                if (!(method == null))
                                {
                                    Invoker value = new Invoker
                                    {
                                        Assembly = assembly.Location,
                                        Type = type.FullName,
                                        Method = method.Name,
                                        AppName = type.Name,
                                        BinPath = ExecutionService.Domain.privateBinPath,
                                        Parameters = parameters,
                                        ConfigurationFile = "ServiceCore.dll.config"
                                    };
                                    dictionary.Add(type.Name, value);
                                }
                            }
                            catch (AmbiguousMatchException)
                            {
                            }
                        }
                    }
                    catch
                    {
                    }
                }
                AppDomain.CurrentDomain.SetData("returns", dictionary);
            }

            ~Domain()
            {
                this.Dispose(false);
            }

            public void Dispose()
            {
                this.Dispose(true);
            }

            private void Dispose(bool disposing)
            {
                if (this.disposed)
                {
                    return;
                }
                this.disposed = true;
                try
                {
                    AppDomain.Unload(this.domain);
                }
                catch (CannotUnloadAppDomainException)
                {
                }
            }

            private static readonly string privateBinPath = string.Format("{0};{1}", Path.Combine(Environment.CurrentDirectory, (IntPtr.Size == 4) ? "x86" : "x64"), Environment.CurrentDirectory);

            private AppDomain domain;

            private bool disposed;
        }
    }

    public static class CLRUtil
    {
        public static IEnumerable<AppDomain> EnumAppDomains()
        {
            // Obtain ICLRMetaHost interface
            object objHost;
            int hr = CLRCreateInstance(ref CLSID_CLRMetaHost, ref IID_CLRMetaHost, out objHost);
            if (hr < 0) throw new COMException("Cannot create meta host", hr);
            var host = (ICLRMetaHost)objHost;

            // Obtain ICLRRuntimeInfo interface
            var vers = Environment.Version;
            var versString = string.Format("v{0}.{1}.{2}", vers.Major, vers.Minor, vers.Build);
            var objRuntime = host.GetRuntime(versString, ref IID_CLRRuntimeInfo);
            var runtime = (ICLRRuntimeInfo)objRuntime;
            bool started;
            uint flags;
            runtime.IsStarted(out started, out flags);
            if (!started) throw new COMException("CLR not started??");

            // Obtain legacy ICorRuntimeHost interface and iterate appdomains
            var V2Host = (ICorRuntimeHost)runtime.GetInterface(ref CLSID_CorRuntimeHost, ref IID_CorRuntimeHost);
            IntPtr hDomainEnum;
            V2Host.EnumDomains(out hDomainEnum);
            for (;;)
            {
                _AppDomain domain = null;
                V2Host.NextDomain(hDomainEnum, out domain);
                if (domain == null) break;
                yield return (AppDomain)domain;
            }
            V2Host.CloseEnum(hDomainEnum);
        }

        private static Guid CLSID_CLRMetaHost = new Guid(0x9280188d, 0xe8e, 0x4867, 0xb3, 0xc, 0x7f, 0xa8, 0x38, 0x84, 0xe8, 0xde);
        private static Guid IID_CLRMetaHost = new Guid(0xD332DB9E, 0xB9B3, 0x4125, 0x82, 0x07, 0xA1, 0x48, 0x84, 0xF5, 0x32, 0x16);
        private static Guid IID_CLRRuntimeInfo = new Guid(0xBD39D1D2, 0xBA2F, 0x486a, 0x89, 0xB0, 0xB4, 0xB0, 0xCB, 0x46, 0x68, 0x91);
        private static Guid CLSID_CorRuntimeHost = new Guid(0xcb2f6723, 0xab3a, 0x11d2, 0x9c, 0x40, 0x00, 0xc0, 0x4f, 0xa3, 0x0a, 0x3e);
        private static Guid IID_CorRuntimeHost = new Guid(0xcb2f6722, 0xab3a, 0x11d2, 0x9c, 0x40, 0x00, 0xc0, 0x4f, 0xa3, 0x0a, 0x3e);

        [DllImport("mscoree.dll")]
        private static extern int CLRCreateInstance(ref Guid clsid, ref Guid iid,
            [MarshalAs(UnmanagedType.Interface)] out object ptr);

        [ComImport, Guid("D332DB9E-B9B3-4125-8207-A14884F53216"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
        private interface ICLRMetaHost
        {
            [return: MarshalAs(UnmanagedType.Interface)]
            object GetRuntime(string version, ref Guid iid);
            // Rest omitted
        }

        [ComImport, Guid("BD39D1D2-BA2F-486a-89B0-B4B0CB466891"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
        private interface ICLRRuntimeInfo
        {
            void GetVersionString(char[] buffer, int bufferLength);
            void GetRuntimeDirectory(char[] buffer, int bufferLength);
            bool IsLoaded(IntPtr hProcess);
            void LoadErrorString(uint id, char[] buffer, int bufferLength, int lcid);
            void LoadLibrary(string path, out IntPtr hMdodule);
            void GetProcAddress(string name, out IntPtr addr);
            [return: MarshalAs(UnmanagedType.Interface)]
            object GetInterface(ref Guid clsid, ref Guid iid);
            bool IsLoadable();
            void SetDefaultStartupFlags(uint flags, string configFile);
            void GetDefaultStartupFlags(out uint flags, char[] configFile, int configFileLength);
            void BindAsLegacyV2Runtime();
            void IsStarted(out bool started, out uint flags);
        }

        [ComImport, Guid("CB2F6722-AB3A-11d2-9C40-00C04FA30A3E"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
        private interface ICorRuntimeHost
        {
            void CreateLogicalThreadState();
            void DeleteLogicalThreadState();
            void SwitchinLogicalThreadState(IntPtr cookie);
            void SwitchoutLogicalThreadState(out IntPtr cookie);
            void LocksHeldByLogicalThread(out int count);
            void MapFile(IntPtr hFile, out IntPtr address);
            void GetConfiguration(out IntPtr config);
            void Start();
            void Stop();
            void CreateDomain(string name, object identity, out _AppDomain domain);
            void GetDefaultDomain(out _AppDomain domain);
            void EnumDomains(out IntPtr hEnum);
            void NextDomain(IntPtr hEnum, out _AppDomain domain);
            void CloseEnum(IntPtr hEnum);
            // rest omitted
        }
    }
}
