using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoPractice
{
    class Program
    {
        static void Main(string[] args)
        {
           MainAsync(args).Wait();
            Console.WriteLine("Press Enter");
            Console.ReadLine();
        }

        static async Task MainAsync(string[] args)
        {
            //ConnectionString
            var connectionString = "mongodb://localhost:27017";

            //First createt the client. singleton in IoC container or 
            //static instance like this are good approaches
            var client = new MongoClient(connectionString);

            //Both the db and col are thread safe so they, like the client, can be stored globaly
            var db = client.GetDatabase("school");
            var col = db.GetCollection<BsonDocument>("students");

            //Method 1 
            #region one way to build a collection
            //using (var cursor = await col.Find(new BsonDocument()).ToCursorAsync())
            //{
            //    while (await cursor.MoveNextAsync())
            //    {
            //        foreach (var student in cursor.Current)
            //        {
            //            if (true)
            //            {

            //            }

            //            Console.WriteLine(student);
            //        }
            //    }
            //}
            #endregion

            //Method 2, store in memory as list
            #region another way to build a collection
            //var list = await col.Find(new BsonDocument()).ToListAsync();
            //foreach (var doc in list)
            //{
            //    Console.WriteLine(doc);
            //}
            #endregion

            //Method 3
            #region Final way to build a collection
            //Foreach document, run the callback method passed into th ForEachAsync
            //await col.Find(new BsonDocument()).ForEachAsync(doc => Console.WriteLine(doc));
            #endregion



        }
    }
}
