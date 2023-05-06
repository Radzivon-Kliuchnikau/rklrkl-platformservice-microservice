using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using PlatformService.AsyncDataServices;
using PlatformService.Data;
using PlatformService.Dtos;
using PlatformService.Models;
using PlatformService.SyncDataServices.Http;

namespace PlatformService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PlatformsController : ControllerBase
    {
        private readonly IPlatformRepo _repo;
        private readonly IMapper _mapper;
        private readonly ICommandDataClient _commandDataClient;
        private readonly IMessageBusClient _messageBusClient;

        public PlatformsController(
            IPlatformRepo repo, 
            IMapper mapper, 
            ICommandDataClient commandDataClient, 
            IMessageBusClient messageBusClient)
        {
            _repo = repo;
            _mapper = mapper;
            _commandDataClient = commandDataClient;
            _messageBusClient = messageBusClient;
        }

        [HttpGet]
        public ActionResult<IEnumerable<PlatformReadDto>> GetPlatforms()
        {
            Console.WriteLine("---> Getting platforms");

            var platforms = _repo.GetAllPlatforms();

            return Ok(_mapper.Map<IEnumerable<PlatformReadDto>>(platforms));
        }

        [HttpGet("{id}", Name = "GetPlatformById")]
        public ActionResult<PlatformReadDto> GetPlatformById(int id)
        {
            var platformModel = _repo.GetPlatformById(id);

            if (platformModel != null)
            {
                return Ok(_mapper.Map<PlatformReadDto>(platformModel));
            }

            return NotFound();
        }

        [HttpPost]
        public async Task<ActionResult<PlatformReadDto>> CreatePlatform(PlatformCreateDto platformCreateDto)
        {
            var platformModel = _mapper.Map<Platform>(platformCreateDto);

            _repo.CreatePlatform(platformModel);
            _repo.SaveChanges();

            var platformReadDto = _mapper.Map<PlatformReadDto>(platformModel);

            // Send SYNC message to CommandService
            try
            {
                await _commandDataClient.SendPlatformToCommand(platformReadDto);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Could not send synchronusly to CommandService: {ex.Message}");
            }

            // Send ASYNC message to CommandService
            try
            {
                var platformPublishedDto = _mapper.Map<PlatformPublishedDto>(platformReadDto);
                platformPublishedDto.Event = "Platform_Published";

                _messageBusClient.PublishNewPlatform(platformPublishedDto);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Could not send asynchronusly to CommandService: {ex.Message}");
            }

            return CreatedAtRoute(nameof(GetPlatformById), new { Id = platformModel.Id }, platformReadDto);
        }
    }
}