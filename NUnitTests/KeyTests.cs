using Shouldly;
using sqlutil;

namespace NUnitTests
{
    [TestFixture]
    public class KeyTests
    {
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            Conn.Load();
        }

        [Order(1)]
        [Test]
        public void KeyChecks()
        {
            Conn.List().Count.ShouldBe(2);

            Conn.Add("tempkey", "asdf");
            Conn.List().Count.ShouldBe(3);

            Conn.Get("tempkey").ShouldBe("asdf");

            Conn.Remove("not_a_real_key");
            Conn.List().Count.ShouldBe(3);

            Conn.Remove("tempkey");
            Conn.List().Count.ShouldBe(2);

            Conn.Get("tempkey", "").ShouldBe("");

            Conn.Get("conn_string would go here").ShouldBe("conn_string would go here");
        }
    }
}
