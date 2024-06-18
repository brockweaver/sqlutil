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

            Should.Throw(() =>
            {
                Conn.Add("bad=name", "ignored");
            }, typeof(InvalidOperationException));

            Should.Throw(() =>
            {
                Conn.Add("bad;name", "ignored");
            }, typeof(InvalidOperationException));

            Conn.Add("tempkey", "asdf");
            Conn.List().Count.ShouldBe(3);

            Conn.Get("tempkey").ShouldBe("asdf");

            Should.NotThrow(() =>
            {
                Conn.Remove("not_a_real_key");
            });

            Should.Throw(() =>
            {
                Conn.Get("invalid_key_name");
            }, typeof(InvalidOperationException));

            Conn.List().Count.ShouldBe(3);

            Conn.Remove("tempkey");
            Conn.List().Count.ShouldBe(2);

        }
    }
}
