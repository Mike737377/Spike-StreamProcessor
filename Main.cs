using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamProcessor
{
    public static class Program
    {
        private static readonly Random _rand = new Random();

        public static void Main(string[] args)
        {
            var stream = new PersonStream();

            stream.AddIndexer(new FirstLetterIndexer());
            stream.AddIndexer(new JohnIndexer());
            stream.AddAnalyzer(new JohnAnalyzer());
            stream.AddAnalyzer(new FirstAndLastLetterOf4Matcher());
            stream.AddAnalyzer(new AgeAnalyzer());

            Console.WriteLine("Inserting ppl");
            Enumerable.Range(0, 300000).Each(x => stream.PutRecord(CreatePerson()));
            stream.PutRecord(new Person { Name = "John", Age = 35, Expected = false });
            Enumerable.Range(0, 300000).Each(x => stream.PutRecord(CreatePerson()));
            stream.PutRecord(new Person { Name = "John", Age = 2, Expected = true });
            Enumerable.Range(0, 300000).Each(x => stream.PutRecord(CreatePerson()));
            stream.PutRecord(new Person { Name = "John", Age = 31, Expected = false });

            Console.WriteLine("Querying records");
            var stopwatch = Stopwatch.StartNew();
            var results = stream.Query(new { Name = "john" }, x => x.Age == 2);
            stopwatch.Stop();
            Console.WriteLine("Time: {0}s", stopwatch.Elapsed.TotalMilliseconds / 1000);

            foreach (var a in results)
            {
                Console.WriteLine(a);
            }
        }

        private static Person CreatePerson()
        {
            return new Person { Name = Guid.NewGuid().ToString().Substring(0, 6), Age = _rand.Next(1, 100), Expected = false };
        }
    }

    public class Person
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public bool Expected { get; set; }

        public override string ToString()
        {
            return string.Format("{0}, {1}... {2}", Name, Age, Expected);
        }
    }

    public class PersonStream : Stream<Person>
    {
    }

    public class JohnIndexer : StreamIndexer<Person>
    {
        public JohnIndexer()
            : base(x => new { Name = x.Name.ToLowerInvariant() })
        { }
    }

    public class FirstLetterIndexer : StreamIndexer<Person>
    {
        public FirstLetterIndexer()
            : base(x => new { Name = x.Name.Substring(0, 1).ToLowerInvariant() })
        { }
    }

    public class FirstAndLastLetterOf4Matcher : IIndexStreamAnalyzer<Person>, IFilteredStreamAnalyzer<Person>
    {
        public object OnlyOnIndex()
        {
            return new { Name = "4" };
        }

        public void Analyze(IQueryableStream<Person> stream, Person item)
        {
            //Console.WriteLine("4..4: {0}", item.Name);
        }

        public bool CanAnalyze(Person item)
        {
            return item.Name.EndsWith("4");
        }
    }

    public class JohnAnalyzer : IIndexStreamAnalyzer<Person>
    {
        public object OnlyOnIndex()
        {
            return new { Name = "john" };
        }

        public void Analyze(IQueryableStream<Person> stream, Person item)
        {
            if (item.Name == "John")
            {
                Console.WriteLine("Found john!!!!!!");
            }

            var stopwatch = Stopwatch.StartNew();
            Console.WriteLine("Johns found: {0}", stream.Query(new { Name = "john" }, x => true).Count());
            stopwatch.Stop();
            Console.WriteLine("Time: {0}s", stopwatch.Elapsed.TotalMilliseconds / 1000);
        }
    }

    public class AgeAnalyzer : IStreamAnalyzer<Person>
    {
        private int _totalPeople = 0;
        private decimal _totalAge = 0;

        public void Analyze(IQueryableStream<Person> stream, Person item)
        {
            _totalPeople++;
            _totalAge += item.Age;
            var averageAge = _totalAge / _totalPeople;

            Console.WriteLine("{0}... Avg: {1}", _totalPeople, averageAge);
        }
    }
}