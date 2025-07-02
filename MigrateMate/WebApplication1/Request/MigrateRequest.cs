using MigrateApi.Connectors;

namespace MigrateApi.Request
{
    public class MigrateRequest
    {
        public MigrateRequestDatabase FromDb { get; set; }
        public MigrateRequestDatabase ToDb { get; set; }
        public MigrateRequestConfig Config { get; set; }
    }

    public class MigrateRequestDatabase
    {
        public string ConnectionString { get; set; }
        public DbType DbType { get; set; }
    }

    public class MigrateRequestConfig
    {
        public bool NoRelations { get; set; }
        public bool SaveRelations { get; set; }
        public bool DeletFromDb { get; set; }

    }
}
