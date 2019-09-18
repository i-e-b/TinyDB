using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace TinyDbTests
{
    [TestFixture]
    public class PathIndexingTests
    {
        [Test]
        public void can_add_keys_to_tree ()
        {
            var subject = new PathTrie();

            subject.Add("my/path/1", "value1");
            subject.Add("my/other/path", "value2");

            Console.WriteLine(subject.DiagnosticString());
        }

        [Test]
        public void stress_test () {
            var subject = new PathTrie();

            subject.Add("start", "start value");

            long totalBytes = 0;
            for (int i = 0; i < 1000; i++)
            {
                var newKey = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
                var newValue = Convert.ToBase64String(Guid.NewGuid().ToByteArray());

                totalBytes += newKey.Length;
                totalBytes += newValue.Length;

                subject.Add(newKey, newValue);
            }

            subject.Add("end", "end value");
            
            Assert.That(subject.Get("start"), Is.EqualTo("start value"));
            Assert.That(subject.Get("end"), Is.EqualTo("end value"));

            
            using (var ms = new MemoryStream()) {
                subject.WriteTo(ms);
                Console.WriteLine($"Produced {totalBytes} bytes");
                Console.WriteLine($"Stored {ms.Length} bytes");
            }

            Console.WriteLine(subject.DiagnosticString());
        }

        [Test]
        public void can_query_keys_for_values ()
        {
            var subject = new PathTrie();

            subject.Add("my/path/1", "value1");
            subject.Add("my/path/2", "value2");
            subject.Add("my/other/path", "value3");
            subject.Add("my/other/path/longer", "value4");

            var r1 = subject.Get("my/path/2");
            var r2 = subject.Get("my/other/path");
            var r3 = subject.Get("not/here");

            Assert.That(r1, Is.EqualTo("value2"));
            Assert.That(r2, Is.EqualTo("value3"));
            Assert.That(r3, Is.Null);
        }

        [Test]
        public void can_remove_values_from_keys () {
            // Note -- we don't actually remove the key, just the value
            // This is the same as setting the value to null.

            var subject = new PathTrie();

            subject.Add("my/path/1", "value1");
            subject.Add("my/path/2", "value2");
            subject.Add("my/other/path", "value3");
            subject.Add("my/other/path/longer", "value4");

            var r1 = subject.Get("my/path/2");
            Assert.That(r1, Is.EqualTo("value2"));


            subject.Delete("my/path/2");

            var r2 = subject.Get("my/other/path");
            var r3 = subject.Get("my/path/1");
            var r4 = subject.Get("my/path/2");

            Assert.That(r2, Is.EqualTo("value3"));
            Assert.That(r3, Is.EqualTo("value1"));
            Assert.That(r4, Is.Null);
        }

        [Test]
        public void can_output_to_a_stream ()
        {
            var subject = new PathTrie();

            subject.Add("my/path/1", "value1");
            subject.Add("my/path/2", "value2");
            subject.Add("my/other/path", "value3");
            subject.Add("my/other/path/longer", "value4");

            using (var ms = new MemoryStream()) {
                subject.WriteTo(ms);
                Assert.That(ms.Length, Is.GreaterThan(10));

                Console.WriteLine($"Wrote {ms.Length} bytes");
            }
        }

        [Test]
        public void can_read_from_a_stream () 
        {
            var source = new PathTrie();

            source.Add("my/path/1", "value1");
            source.Add("my/path/2", "value2");
            source.Add("my/other/path", "value3");
            source.Add("my/other/path/longer", "value4");

            var target = new PathTrie();
            using (var ms = new MemoryStream()) {
                source.WriteTo(ms);
                ms.Seek(0, SeekOrigin.Begin);
                target.Import(ms);
            }

            Assert.That(target.Get("my/path/1"), Is.EqualTo("value1"));
            Assert.That(target.Get("my/path/2"), Is.EqualTo("value2"));
            Assert.That(target.Get("my/other/path"), Is.EqualTo("value3"));
            Assert.That(target.Get("my/other/path/longer"), Is.EqualTo("value4"));
        }
    }

    public class PathTrie
    {
        class Node
        {
            // char at this point
            public char Ch;

            // Indexes into the parent array
            public int Left, Match, Right;

            // If null, this is not a path endpoint
            public int DataIdx;

            public Node()
            {
                Left = Match = Right = DataIdx = -1;
            }
        }


        private readonly List<Node> _nodes;
        private readonly List<string> _entries;

        public PathTrie()
        {
            _nodes = new List<Node>();
            _entries = new List<string>();
        }

        /// <summary>
        /// Insert a path/value pair into the index.
        /// If a value already existed for the path, it will be replaced and the old value returned
        /// </summary>
        public string Add(string path, string value) {
            if (string.IsNullOrEmpty(path)) return null;

            var nodeIdx = EnsurePath(path);

            var node = _nodes[nodeIdx];
            var oldValue = GetValue(node.DataIdx);
            SetValue(nodeIdx, value);
            return oldValue;
        }

        /// <summary>
        /// Get a value by exact path.
        /// If the path has no value, NULL will be returned
        /// </summary>
        public string Get(string exactPath)
        {
            if (string.IsNullOrEmpty(exactPath)) return null;

            var nodeIdx = WalkPath(exactPath);
            if (nodeIdx < 0 || nodeIdx >= _nodes.Count) return null;
            var node = _nodes[nodeIdx];
            return GetValue(node.DataIdx);
        }

        /// <summary>
        /// Delete the value for a key, by exact path.
        /// If the path has no value, nothing happens
        /// </summary>
        public void Delete(string exactPath)
        {
            if (string.IsNullOrEmpty(exactPath)) return;

            var nodeIdx = WalkPath(exactPath);
            if (nodeIdx < 0 || nodeIdx >= _nodes.Count) return;
            var node = _nodes[nodeIdx];
            SetValue(node.DataIdx, null);
            node.DataIdx = -1;
        }
        
        private int WalkPath(string path)
        {
            int curr = 0;
            int next = 0;

            int cpos = 0; // char offset
            while (cpos < path.Length)
            {
                if (next < 0) return -1;
                curr = next;

                var ch = path[cpos];
                next = ReadStep(curr, ch, ref cpos);
                 
            }
            return curr;
        }

        private int ReadStep(int idx, char ch, ref int matchIncr)
        {
            if (_nodes.Count < 1) { // empty
                return -1;
            }

            var inspect = _nodes[idx];

            if (inspect.Ch == 0) { // empty match. No key here.
                return -1;
            }

            if (inspect.Ch == ch) {
                matchIncr++;
                return inspect.Match;
            }

            // can't follow the straight line. Need to branch

            if (ch < inspect.Ch) {
                // switch left
                return inspect.Left;
            }

            if (ch > inspect.Ch) {
                // switch right
                return inspect.Right;
            }

            throw new Exception("Invalid");
        }

        private int EnsurePath(string path)
        {
            int curr = 0;
            int next = 0;

            int cpos = 0; // char offset
            while (cpos < path.Length)
            {
                curr = next;

                var ch = path[cpos];
                next = BuildStep(curr, ch, ref cpos);
            }
            return curr;
        }

        private int BuildStep(int idx, char ch, ref int matchIncr)
        {
            if (_nodes.Count < 1) { // empty
                return NewIndexNode(ch);
            }

            var inspect = _nodes[idx];

            if (inspect.Ch == 0) { // empty match. Fill it in
                inspect.Ch = ch;
                if (inspect.Match > -1) throw new Exception("invalid");
                inspect.Match = NewEmptyIndex(); // next empty match
                return idx;
            }

            if (inspect.Ch == ch) {
                matchIncr++;
                if (inspect.Match < 0) {
                    inspect.Match = NewEmptyIndex();
                }
                return inspect.Match;
            }

            // can't follow the straight line. Need to branch

            if (ch < inspect.Ch) {
                // switch left
                if (inspect.Left < 0) {
                    // add new node for this value, increment match
                    inspect.Left = NewIndexNode(ch);
                    _nodes[inspect.Left].Match = NewEmptyIndex();
                }
                return inspect.Left;
            }

            if (ch > inspect.Ch) {
                // switch right
                if (inspect.Right < 0) {
                    // add new node for this value, increment match
                    inspect.Right = NewIndexNode(ch);
                    _nodes[inspect.Right].Match = NewEmptyIndex();
                }
                return inspect.Right;
            }

            throw new Exception("Invalid");
        }

        private int NewIndexNode(char ch)
        {
            var node = new Node();
            node.Ch = ch;
            var idx = _nodes.Count;
            _nodes.Add(node);
            return idx;
        }
        
        private int NewEmptyIndex()
        {
            var node = new Node();
            node.Ch = (char) 0;
            var idx = _nodes.Count;
            _nodes.Add(node);
            return idx;
        }

        private void SetValue(int nodeIdx, string value)
        {
            if (nodeIdx < 0) throw new Exception("node index makes no sense");
            if (nodeIdx >= _nodes.Count) throw new Exception("node index makes no sense");

            var newIdx = _entries.Count;
            _entries.Add(value);

            _nodes[nodeIdx].DataIdx = newIdx;
        }

        private string GetValue(int nodeDataIdx)
        {
            if (nodeDataIdx < 0) return null;
            if (nodeDataIdx >= _entries.Count) return null;
            return _entries[nodeDataIdx];
        }

        public string DiagnosticString()
        {
            var sb = new StringBuilder();

            sb.AppendLine("INDEX: ");
            int i = 0;
            foreach (var node in _nodes)
            {
                sb.Append("    ");
                sb.Append(i);
                sb.Append("['");
                sb.Append(node.Ch);
                sb.Append("', D=");
                sb.Append(node.DataIdx);
                sb.Append(", L=");
                sb.Append(node.Left);
                sb.Append(", M=");
                sb.Append(node.Match);
                sb.Append(", R=");
                sb.Append(node.Right);
                sb.AppendLine("];");
                i++;
            }

            sb.AppendLine("DATA: ");
            i = 0;
            foreach (var entry in _entries)
            {
                sb.Append("    ");
                sb.Append(i);
                sb.Append("[");
                sb.Append(entry);
                sb.AppendLine("];");
                i++;
            }

            return sb.ToString();
        }

        // Flag values
        const byte HAS_MATCH = 1 << 0;
        const byte HAS_LEFT = 1 << 1;
        const byte HAS_RIGHT = 1 << 2;
        const byte HAS_DATA = 1 << 3;
        
        const long INDEX_MARKER = 0xFACEFEED; // 32 bits of zero, then the magic number
        const long DATA_MARKER  = 0xBACCFACE;
        const long END_MARKER   = 0xDEADBEEF;

        /// <summary>
        /// Write a serialised form to the stream at its current position
        /// </summary>
        public void WriteTo(Stream stream)
        {
            if (stream == null) return;
            using (var w = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                w.Write(INDEX_MARKER);
                w.Write(_nodes.Count);
                foreach (var node in _nodes) { WriteIndexNode(node, w); }
                
                w.Write(DATA_MARKER);
                w.Write(_entries.Count);
                foreach (var entry in _entries) { WriteDataEntry(entry, w); }
                w.Write(END_MARKER);
            }
        }
        
        /// <summary>
        /// Read a stream (previously written by `WriteTo`) from its current position
        /// into a new index. Will throw an exception if the data is not consistent and complete.
        /// </summary>
        /// <param name="stream"></param>
        public static PathTrie ReadFrom(Stream stream)
        {
            if (stream == null) return null;
            var result = new PathTrie();
            using (var r = new BinaryReader(stream, Encoding.UTF8, true))
            {
                if (r.ReadInt64() != INDEX_MARKER) throw new Exception("Input stream missing index marker");
                var nodeCount = r.ReadInt32();
                if (nodeCount < 0) throw new Exception("Input stream node count invalid");

                for (int i = 0; i < nodeCount; i++)
                {
                    result._nodes.Add(ReadIndexNode(r));
                }
                
                if (r.ReadInt64() != DATA_MARKER) throw new Exception("Input stream missing data marker");
                var entryCount = r.ReadInt32();
                if (entryCount < 0) throw new Exception("Input stream node count invalid");
                
                for (int i = 0; i < entryCount; i++)
                {
                    result._entries.Add(ReadDataEntry(r));
                }
                
                if (r.ReadInt64() != END_MARKER) throw new Exception("Input stream missing end marker");
            }
            return result;
        }

        private static string ReadDataEntry(BinaryReader r)
        {
            return TODO_IMPLEMENT_ME;
        }

        private void WriteDataEntry(string data, BinaryWriter w)
        {
            if (data == null) {
                w.Write((int)-1);
                return;
            }
            var bytes = Encoding.UTF8.GetBytes(data); // we do this to ensure an exact byte count
            w.Write(bytes.Length);
            w.Write(bytes);
        }

        

        private static Node ReadIndexNode(BinaryReader r)
        {
            return TODO_IMPLEMENT_ME;
        }

        private static void WriteIndexNode(Node node, BinaryWriter w)
        {
            byte flags = 0;
            if (node.Match >= 0) flags |= HAS_MATCH;
            if (node.Left >= 0) flags |= HAS_LEFT;
            if (node.Right >= 0) flags |= HAS_RIGHT;
            if (node.DataIdx >= 0) flags |= HAS_DATA;

            w.Write(node.Ch);
            w.Write(flags);

            if (node.Match >= 0) w.Write(node.Match);
            if (node.Left >= 0) w.Write(node.Left);
            if (node.Right >= 0) w.Write(node.Right);
            if (node.DataIdx >= 0) w.Write(node.DataIdx);
        }
    }
}