using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;

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
        public void can_query_for_keys ()
        {
            Assert.Fail("NYI");
        }

        [Test]
        public void can_output_to_a_stream ()
        {
            Assert.Fail("NYI");
        }

        [Test]
        public void can_read_from_a_stream () 
        {
            Assert.Fail("NYI");
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


        private readonly List<Node> _index;
        private readonly List<string> _entries;

        public PathTrie()
        {
            _index = new List<Node>();
            _entries = new List<string>();
        }

        /// <summary>
        /// Insert a path/value pair into the index.
        /// If a value already existed for the path, it will be replaced and the old value returned
        /// </summary>
        public string Add(string path, string value) {
            if (string.IsNullOrEmpty(path)) return null;

            var nodeIdx = EnsurePath(path);

            var node = _index[nodeIdx];
            var oldValue = GetValue(node.DataIdx);
            SetValue(nodeIdx, value);
            return oldValue;
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
                next = Step(curr, ch, ref cpos);
            }
            return curr;
        }

        private int Step(int idx, char ch, ref int matchIncr)
        {
            if (_index.Count < 1) { // empty
                //matchIncr++;
                return NewIndexNode(ch);
            }

            var inspect = _index[idx];

            if (inspect.Ch == 0) { // empty match. Fill it in
                //matchIncr++;
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
                    _index[inspect.Left].Match = NewEmptyIndex();
                }
                return inspect.Left;
            }

            if (ch > inspect.Ch) {
                // switch right
                if (inspect.Right < 0) {
                    // add new node for this value, increment match
                    inspect.Right = NewIndexNode(ch);
                    _index[inspect.Right].Match = NewEmptyIndex();
                }
                return inspect.Right;
            }

            throw new Exception("Invalid");
        }

        private int NewIndexNode(char ch)
        {
            var node = new Node();
            node.Ch = ch;
            var idx = _index.Count;
            _index.Add(node);
            return idx;
        }
        
        private int NewEmptyIndex()
        {
            var node = new Node();
            node.Ch = (char) 0;
            var idx = _index.Count;
            _index.Add(node);
            return idx;
        }

        private bool IsEmpty() { return _index.Count < 1; }

        private void SetValue(int nodeIdx, string value)
        {
            if (nodeIdx < 0) throw new Exception("node index makes no sense");
            if (nodeIdx >= _index.Count) throw new Exception("node index makes no sense");

            var newIdx = _entries.Count;
            _entries.Add(value);

            _index[nodeIdx].DataIdx = newIdx;
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
            foreach (var node in _index)
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
    }
}