using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace MongoPractice
{
    public class HW3
    {

        //This Homework task uses the students.json as a mongo database.
        //Importable by cmd: mongoimport --drop -d school -c students filelocation\students.json
        //The task is to remove the lowest homework grade from each student, then to find the student with the highest average(all grade types) after dropping lowest homework.
        //Answer = _iD = 13

        public static void Main(string[] args)
        {
            MainAsync(args).Wait();
            Console.ReadKey();
        }

        public static async Task MainAsync(string[] args)
        {
            var client = new MongoClient();
            var db = client.GetDatabase("school");
            var col = db.GetCollection<Student>("students");

            await col.Find(new BsonDocument()).ForEachAsync(async student =>
            {
                //First get the lowest HW grade by matching the student grade to the gradetype 'homework',
                //Then sort the homeworks by score, default is lowest to highest
                //Finally the first() grabs the first(lowest) homework and stores it in memory as 'lowestHomeworkGrade'
                var lowestHomeworkGrade = student.Grades
                .Where(x => x.Type == GradeType.homework)
                .OrderBy(x => x.Score).First();

                //Then, there are many ways to remove and update the grades array on the student object/document

                //option 1: remove it server-side
                //await col.UpdateOneAsync
                //(
                //    filter: x => x.Id == student.Id,
                //    update: Builders<Student>.Update.PullFilter
                //    (
                //        field: x => x.Grades,
                //        filter: score.Score == lowestHomeworkGrade.Score && score.Type == GradeType.homework
                //    )
                //);

                //option 2: remove it client-side and replace only the scores
                //student.Grades.Remove(lowestHomeworkGrade);
                //await col.UpdateOneAsync
                //    (
                //        filter: x => x.Id == student.Id,
                //        update: Builders<Student>.Update.Set(x => x.Grades, student.Grades)
                //    );

                //option3: Remove it Client-side and replace the whole student
                student.Grades.Remove(lowestHomeworkGrade);
                await col.ReplaceOneAsync
                    (
                        filter: x => x.Id == student.Id,
                        replacement: student
                    );

            });


            // translation from the aggregation querry to average out the remaining grades, then sort from highest to lowest grade, then to return the first(highest) average
            var result = await col.Aggregate()
                .Unwind(x => x.Grades)
                .Group(new BsonDocument {
                        {"_id", "$_id" },
                        { "average", new BsonDocument("$avg","$scores.score")}
                })
                .Sort(new BsonDocument("average", -1))
                .FirstAsync();

            Console.WriteLine(result);
        }

        private class Student
        {
            public int Id { get; set; }

            [BsonElement("name")]
            public string Name { get; set; }

            [BsonElement("scores")]
            public List<Grade> Grades { get; set; }
        }

        private class Grade
        {
            [BsonElement("type")]
            [BsonRepresentation(BsonType.String)]
            public GradeType Type {get; set;}

            [BsonElement("score")]
            public double Score { get; set; }
        }

        public enum GradeType
        {
            homework,
            exam,
            quiz
        }
    }
}
