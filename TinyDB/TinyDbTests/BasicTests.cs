using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using TinyDB;
// ReSharper disable PossibleNullReferenceException

namespace TinyDbTests
{
    [TestFixture]
    public class BasicTests
    {
        const string TestFilePath = @"C:\Temp\TinyDB\testfile.db";
        const string message = "Hello, world. This is my binary test";

        [SetUp]
        public void Cleanup()
        {
            if (File.Exists(TestFilePath)) File.Delete(TestFilePath);
        }

        [Test]
        public void can_connect_to_a_db_file ()
        {
            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");
            }
        }

        [Test]
        public void can_store_and_recover_a_file ()
        {
            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var ms = MakeSourceStream();

                var info = subject.Store("test_name", ms);

                Assert.That(info, Is.Not.Null, "Storage result was invalid");

                var ms2 = new MemoryStream();
                var info2 = subject.Read(info.ID, ms2);
                ms2.Seek(0, SeekOrigin.Begin);
                
                Console.WriteLine($"{info.ID} -> {info2.ID}");

                var result = Encoding.UTF8.GetString(ms2.ToArray());
                Assert.That(result, Is.EqualTo(message), "message changed");
                Assert.That(info.ID, Is.EqualTo(info2.ID), "Id changed");
            }
        }

        [Test]
        public void can_read_file_info_with_null_stream ()
        {
            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var ms = MakeSourceStream();

                var info = subject.Store("test_name", ms);

                Assert.That(info, Is.Not.Null, "Storage result was invalid");

                var info2 = subject.Read(info.ID, null);
                
                Console.WriteLine($"{info.ID} -> {info2.ID}");
                Console.WriteLine($"{info.FileName} -> {info2.FileName}");

                Assert.That(info.ID, Is.EqualTo(info2.ID), "Id changed");
                Assert.That(info.FileName, Is.EqualTo(info2.FileName), "Id changed");
            }
        }
        
        [Test]
        public void deleting_a_file_makes_it_unavailable ()
        {
            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var ms = MakeSourceStream();

                var info = subject.Store("test_name", ms);

                Assert.That(info, Is.Not.Null, "Storage result was invalid");

                var info2 = subject.Read(info.ID, null);
                Assert.That(info2, Is.Not.Null, "File did not store");

                var ok = subject.Delete(info2.ID);
                var info3 = subject.Read(info.ID, null);
                Assert.That(ok, Is.True, "delete failed");
                Assert.That(info3, Is.Null, "query succeeded, but should have failed");
            }
        }


        private static MemoryStream MakeSourceStream()
        {
            var ms = new MemoryStream();
            var buf = Encoding.UTF8.GetBytes(message);
            ms.Write(buf, 0, buf.Length);
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

    }

}
