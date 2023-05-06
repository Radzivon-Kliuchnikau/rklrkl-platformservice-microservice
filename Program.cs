using Microsoft.EntityFrameworkCore;
using PlatformService.AsyncDataServices;
using PlatformService.Data;
using PlatformService.SyncDataServices.Grpc;
using PlatformService.SyncDataServices.Http;

var builder = WebApplication.CreateBuilder(args);

if (builder.Environment.IsProduction())
{
    Console.WriteLine("---> Using in SQl Server DB");
    builder.Services.AddDbContext<AppDbContext>(options =>
    {
        options.UseSqlServer(builder.Configuration.GetConnectionString("PlatformsConnection"));
    });
}
else
{
    Console.WriteLine("---> Using in Memory DB");
    builder.Services.AddDbContext<AppDbContext>(options =>
    {
        options.UseInMemoryDatabase("Db");
    });
}

builder.Services.AddScoped<IPlatformRepo, PlatformRepo>();
builder.Services.AddSingleton<IMessageBusClient, MessageBusClient>();
builder.Services.AddHttpClient<ICommandDataClient, HttpCommandDataClient>();

builder.Services.AddGrpc();

builder.Services.AddControllers();

builder.Services.AddAutoMapper(AppDomain.CurrentDomain.GetAssemblies());


var app = builder.Build();

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();
app.MapGrpcService<GrpcPlatformService>();

app.MapGet("protos/platforms.proto", async context =>
{
    await context.Response.WriteAsync(File.ReadAllText("Protos/platforms.proto"));
});

PrepDb.PrepPopulation(app, app.Environment.IsProduction());

app.Run();
