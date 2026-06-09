using Microsoft.Extensions.Logging;
using Serilog;
using WS_Haimdall;
using WS_Haimdall.Model_Class;




string logFolder = Path.Combine(AppContext.BaseDirectory, "Logs");

Directory.CreateDirectory(logFolder);

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File(
        path: Path.Combine(logFolder, "log-.txt"),
        rollingInterval: RollingInterval.Day,
        retainedFileCountLimit: 30,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}",
        shared: true)
    .CreateLogger();


try
{

    var builder = Host.CreateApplicationBuilder(args);

    builder.Logging.ClearProviders();
    builder.Logging.AddSerilog(Log.Logger);


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

}
catch (Exception ex)
{
    Log.Fatal(ex, "Service terminated unexpectedly");
}
finally
{
    
    //if the service crashed with some reasons,
    //sometimes it the message will be still in memory,
    //so by having this line, we it ensures that all the logs are going to be
    //printed in file, before closing the application.
    Log.CloseAndFlush();
}