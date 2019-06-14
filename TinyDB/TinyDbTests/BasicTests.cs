using NUnit.Framework;
using TinyDB;
// ReSharper disable PossibleNullReferenceException

namespace TinyDbTests
{
    [TestFixture]
    public class BasicTests
    {

        [Test]
        public void can_connect_to_a_db_file ()
        {
            var subject = Datastore.TryConnect("testfile.db");
            Assert.That(subject, Is.Not.Null, "Database connector failed");
        }
    }
}
