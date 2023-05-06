using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PlatformService.Models;

namespace PlatformService.Data
{
    public class PlatformRepo : IPlatformRepo
    {
        private readonly AppDbContext _context;

        public PlatformRepo(AppDbContext context)
        {
            _context = context;
        }

        public void CreatePlatform(Platform platform)
        {
            if (platform == null)
            {
                throw new ArgumentNullException(nameof(platform));
            }

            _context.Platforms.Add(platform);
        }

        public IEnumerable<Platform> GetAllPlatforms()
        {
            var platforms = _context.Platforms.ToList();

            return platforms;
        }

        public Platform? GetPlatformById(int id)
        {
            var platform = _context.Platforms.FirstOrDefault(platform => platform.Id == id);

            return platform;
        }

        public bool SaveChanges()
        {
            return (_context.SaveChanges() >= 0);
        }
    }
}