using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MigrateApi.Connectors;
using MigrateApi.Request;

namespace MigrateApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MigrationController : ControllerBase
    {

        [HttpPost("Start")]
        public async Task<IActionResult> GetByFilter([FromBody] MigrateRequest request)
        {
            DbManager dbManager = new DbManager(request);

            dbManager.MigrateData();

            return new JsonResult("", new JsonSerializerOptions() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        }

    }
}