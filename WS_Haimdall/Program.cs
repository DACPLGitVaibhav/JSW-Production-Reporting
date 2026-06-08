using Microsoft.Extensions.Logging;
using Serilog;
using WS_Haimdall;
using WS_Haimdall.Model_Class;

string basePath = AppContext.BaseDirectory;
string logFolder = Path.Combine(basePath, "logs");
Directory.CreateDirectory(logFolder);

string logFile = Path.Combine(logFolder, "app_log.txt");

Log.Logger = new LoggerConfiguration()
   .MinimumLevel.Information()
   .MinimumLevel.Override("Microsoft.Hosting.Lifetime", Serilog.Events.LogEventLevel.Fatal)
   .WriteTo.Console()
   //.WriteTo.File(logFile, rollingInterval: RollingInterval.Year)

   //.WriteTo.Logger(lc => lc
   //     .Filter.ByIncludingOnly(e => e.Properties.ContainsKey("Line") &&
   //                                  e.Properties["Line"].ToString().Contains("FF"))
   //     .WriteTo.File(
   //         Path.Combine(logFolder, "FF.txt"),
   //         fileSizeLimitBytes: 50 * 1024, //1_000_000,   // 1 MB
   //         rollOnFileSizeLimit: true,
   //         rollingInterval: RollingInterval.Infinite))

   //.WriteTo.Map(
   // keyPropertyName: "Line",
   // defaultKey: "General",
   // configure: (line, wt) => wt.File(
   //     Path.Combine(logFolder, $"{line}.txt"),
   //     fileSizeLimitBytes: 50 * 1024,  // 50 KB
   //     rollOnFileSizeLimit: true,
   //     rollingInterval: RollingInterval.Infinite,
   //     retainedFileCountLimit: null   // keep all files
   // ))
   .CreateLogger();

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();
// Enable Windows Service
builder.Services.AddWindowsService(options =>
{
    options.ServiceName = "WS_Haimdall";
});
builder.Services.Configure<appSettings>(
    builder.Configuration.GetSection("appSettings"));
var host = builder.Build();
host.Run();
