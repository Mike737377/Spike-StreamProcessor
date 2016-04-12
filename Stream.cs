using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamProcessor
{

    public class Stream<T>
    {
        private readonly ConcurrentDictionary<object, ConcurrentBag<T>> _index = new ConcurrentDictionary<object, ConcurrentBag<T>>();
        private readonly ConcurrentBag<StreamIndexer<T>> _indexers = new ConcurrentBag<StreamIndexer<T>>();
        private readonly ConcurrentBag<IStreamAnalyzer<T>> _analyzers = new ConcurrentBag<IStreamAnalyzer<T>>();

        public void AddIndexer(StreamIndexer<T> indexer)
        {
            _indexers.Add(indexer);
        }

        public void AddAnalyzer(IStreamAnalyzer<T> analyzer)
        {
            _analyzers.Add(analyzer);
        }

        public IEnumerable<T> Query(object index, Func<T, bool> query)
        {
            ConcurrentBag<T> bag;
            if (_index.TryGetValue(index, out bag))
            {                
                var items = bag.ToList().AsEnumerable();

                foreach (var item in items)
                {
                    if (query(item))
                    {
                        yield return item;
                    }
                }
            }
        }

        public void PutRecord(T item)
        {
            _indexers.AsParallel().ForAll(x =>
            {
                var index = x.CreateIndex(item);
                _index.TryAdd(index.Index, new ConcurrentBag<T>());
                _index[index.Index].Add(item);
            });

            _analyzers.AsParallel().ForAll(x =>
            {
                x.Analyze(item);
            });
        }

        public void PutRecords(IEnumerable<T> streamOfItems)
        {
            foreach (var item in streamOfItems)
            {
                PutRecord(item);
            }
        }
    }

    public sealed class StreamIndex<T>
    {
        public StreamIndex(object index, T value)
        {
            Index = index;
            Value = value;
        }

        public object Index { get; private set; }
        public T Value { get; private set; }
    }


    public abstract class StreamIndexer<T>
    {
        private readonly Func<T, object> _indexAlgorithm;

        public StreamIndexer(Func<T, object> indexAlgorithm)
        {
            _indexAlgorithm = indexAlgorithm;
        }

        public StreamIndex<T> CreateIndex(T streamItem)
        {
            return new StreamIndex<T>(_indexAlgorithm(streamItem), streamItem);
        }
    }

    public interface IStreamAnalyzer<T>
    {
        void Analyze(T item);
    }
}
