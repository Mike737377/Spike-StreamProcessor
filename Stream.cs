using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamProcessor
{
    public interface IQueryableStream<T>
    {
        IEnumerable<T> Query(object index);

        IEnumerable<T> Query(object index, Func<T, bool> query);
    }

    public class CachedAnalyzer<T>
    {
        private readonly Func<HashSet<object>, bool> _indexerFunc;
        private readonly Func<T, bool> _filterFunc;

        public CachedAnalyzer(IStreamAnalyzer<T> analyzer)
        {
            Analyzer = analyzer;

            if (analyzer is IFilteredStreamAnalyzer<T>)
            {
                var f = ((IFilteredStreamAnalyzer<T>)analyzer);
                _filterFunc = item => f.CanAnalyze(item);
            }
            else
            {
                _filterFunc = x => true;
            }

            if (analyzer is IIndexStreamAnalyzer<T>)
            {
                var index = ((IIndexStreamAnalyzer<T>)analyzer).OnlyOnIndex();
                _indexerFunc = x => x.Contains(index);
            }
            else
            {
                _indexerFunc = x => true;
            }
        }

        public bool CanAnalyze(HashSet<object> indexes, T item)
        {
            return _indexerFunc(indexes) && _filterFunc(item);
        }

        public IStreamAnalyzer<T> Analyzer { get; private set; }
    }

    public class Stream<T> : IQueryableStream<T>
    {
        private readonly ConcurrentDictionary<object, ConcurrentBag<T>> _index = new ConcurrentDictionary<object, ConcurrentBag<T>>();
        private readonly ConcurrentBag<StreamIndexer<T>> _indexers = new ConcurrentBag<StreamIndexer<T>>();
        private readonly ConcurrentBag<CachedAnalyzer<T>> _analyzers = new ConcurrentBag<CachedAnalyzer<T>>();

        public void AddIndexer(StreamIndexer<T> indexer)
        {
            _indexers.Add(indexer);
        }

        public void AddAnalyzer(IStreamAnalyzer<T> analyzer)
        {
            _analyzers.Add(new CachedAnalyzer<T>(analyzer));
        }

        public IEnumerable<T> Query(object index)
        {
            ConcurrentBag<T> bag;
            if (_index.TryGetValue(index, out bag))
            {
                return bag.AsEnumerable();
            }

            return new T[] { };
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
            var addedIndexes = new ConcurrentBag<object>();

            _indexers.AsParallel().ForAll(x =>
            {
                var indexInfo = x.CreateIndex(item);
                if (indexInfo.Index != null)
                {
                    _index.TryAdd(indexInfo.Index, new ConcurrentBag<T>());
                    _index[indexInfo.Index].Add(item);
                    addedIndexes.Add(indexInfo.Index);
                }
            });

            var indexSet = new HashSet<object>(addedIndexes);

            _analyzers.AsParallel().ForAll(x =>
            {
                if (x.CanAnalyze(indexSet, item))
                {
                    x.Analyzer.Analyze(this, item);
                }
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
        void Analyze(IQueryableStream<T> stream, T item);
    }

    public interface IIndexStreamAnalyzer<T> : IStreamAnalyzer<T>
    {
        object OnlyOnIndex();
    }

    public interface IFilteredStreamAnalyzer<T> : IStreamAnalyzer<T>
    {
        bool CanAnalyze(T item);
    }
}