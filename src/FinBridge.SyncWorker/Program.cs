using FinBridge.SyncWorker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<DynamicsSyncService>();
builder.Services.AddHostedService<InvoiceConsumerWorker>();

var host = builder.Build();
host.Run();
