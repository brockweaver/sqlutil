using Shouldly;
using sqlutil;

namespace NUnitTests
{
    [TestFixture]
    public class Tests
    {
        private static string _srcDb = "";
        private static string _tgtDb = "";
        private static string _exportFile = "test_export.txt";

        [OneTimeSetUp]
        public static void OneTimeSetup()
        {
            Conn.Load();
            _srcDb = Conn.Get("src_db");
            _tgtDb = Conn.Get("tgt_db");

            // create a table for testing
            CreateTest1Table(_srcDb);
            CreateTest1Table(_tgtDb);

            // populate src_db table with some junk data
            AddRow(1, _srcDb);
            AddRow(2, _srcDb);
            AddRow(3, _srcDb);

        }

        [OneTimeTearDown]
        public static void OneTimeTeardown()
        {
            // drop the test table
            Sql.Write($@"
drop table if exists dbo.test_1
", _srcDb);
        }

        private static void AddRow(int id, string dbConn)
        {
            Sql.Write($@"
insert into dbo.test_1 (id, uuid, created_at, name, title, description) 
values (
    {id}, {Guid.NewGuid()}, {DateTime.UtcNow}, {"name " + id}, {"title " + id}, {"description " + id}
)
", dbConn);

        }

        private static void CreateTest1Table(string dbConn)
        {
            // drop any table with same name from a previous run
            Sql.Write($@"
drop table if exists dbo.test_1
", dbConn);

            Sql.Write($@"
create table dbo.test_1 (
    id int,
    uuid uniqueidentifier null,
    created_at datetime2 null,
    name varchar(50) null,
    title char(20) null,
    description nvarchar(max) null
)
", dbConn);

        }

        private class Test1Model
        {
            public int Id { get; set; }
            Guid? Uuid { get; set; }
            DateTime? CreatedAt { get; set; }
            string? Name { get; set; }
            string? Title { get; set; }
            string? Description { get; set; }
        }


        [Order(1)]
        [Test]
        public void ReadDataTest()
        {
            var models = Sql.Read<Test1Model>($@"
select * from dbo.test_1 order by id
", _srcDb);
            models.Count.ShouldBe(3);
            models[0].Id.ShouldBe(1);
            models[1].Id.ShouldBe(2);
            models[2].Id.ShouldBe(3);
        }

        [Order(2)]
        [Test]
        public void ExportDataTest()
        {
            Should.NotThrow(() =>
            {
                Data.Export(_srcDb, _exportFile);
                var fi = new FileInfo(_exportFile);
                Console.WriteLine("export file path = " + fi.FullName);
                fi.Length.ShouldBeGreaterThan(0);
                Sql.ReadValue($"select count(*) from dbo.test_1", _srcDb).ShouldBe(3);
                Sql.ReadValue($"select count(*) from dbo.test_1", _tgtDb).ShouldBe(0);
            });
        }


        [Order(3)]
        [Test]
        public void ImportDataTest()
        {
            Should.NotThrow(() =>
            {
                Data.Import(_exportFile, _tgtDb);
                Sql.ReadValue($"select count(*) from dbo.test_1", _tgtDb).ShouldBe(3);
            });
        }


        [Order(4)]
        [Test]
        public void WipeDataTest()
        {
            Should.NotThrow(() =>
            {
                Data.Wipe(_tgtDb);
                Sql.ReadValue($"select count(*) from dbo.test_1", _tgtDb).ShouldBe(0);
            });
        }

        [Order(5)]
        [Test]
        public void CopyDataTest()
        {
            var tempfile = Path.GetTempFileName();

            Should.NotThrow(() =>
            {
                Data.Export(_srcDb, tempfile);
                File.Exists(tempfile).ShouldBeTrue();

                Sql.ReadValue($"select count(*) from dbo.test_1", _srcDb).ShouldBe(3);
                Sql.ReadValue($"select count(*) from dbo.test_1", _tgtDb).ShouldBe(0);

                Data.Wipe(_tgtDb);
                Sql.ReadValue($"select count(*) from dbo.test_1", _srcDb).ShouldBe(3);
                Sql.ReadValue($"select count(*) from dbo.test_1", _tgtDb).ShouldBe(0);

                Data.Import(tempfile, _tgtDb);
                Sql.ReadValue($"select count(*) from dbo.test_1", _srcDb).ShouldBe(3);
                Sql.ReadValue($"select count(*) from dbo.test_1", _tgtDb).ShouldBe(3);

            });
        }
    }
}