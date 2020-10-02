using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Nest;

namespace ElasticObservableDoubleInsertws
{
    class Program
    {
        static async Task Main(string[] args)
        {
            ElasticClient client = new ElasticClient();

            var list = new List<PriceItem>()
            {
                new PriceItem()
                {
                    Amount = 0,
                    FromDate = DateTime.Now,
                    ToDate = DateTime.Now.AddDays(2),
                    Key = "key",
                }
            };

          //  await ProcessPriceListAsyncInsertsDouble(client, list);

          await ProcessPriceListAsyncInsertsCorrect(client, list);
        }


        public static Task ProcessPriceListAsyncInsertsDouble(ElasticClient client, IEnumerable<PriceItem> prices)
        {
            BulkAllObservable<PriceItem> bulkAllObservable = CreateBulkAllObservable(client, prices);

            bulkAllObservable.Subscribe(new BulkAllObserver(
                onNext: response => Console.WriteLine("Starting next batch"),
                onError: Console.WriteLine,
                onCompleted: () => Console.WriteLine("DONE!")
            ));

            bulkAllObservable.Wait(TimeSpan.FromMinutes(60), null);

            return Task.CompletedTask;
        }

        public static Task ProcessPriceListAsyncInsertsCorrect(ElasticClient client, IEnumerable<PriceItem> prices)
        {
            BulkAllObservable<PriceItem> bulkAllObservable = CreateBulkAllObservable(client, prices);
            
            bulkAllObservable.Wait(TimeSpan.FromMinutes(60), null);

            return Task.CompletedTask;
        }

        private static BulkAllObservable<PriceItem> CreateBulkAllObservable(ElasticClient client, IEnumerable<PriceItem> prices)
        {
            return client.BulkAll(prices, bulkAllDescriptor => bulkAllDescriptor
                .Index("prices")
                .BackOffTime(TimeSpan.FromSeconds(10))
                .BackOffRetries(3)
                .MaxDegreeOfParallelism(Environment.ProcessorCount)
                .Size(1000)
                .RefreshOnCompleted()
                .ContinueAfterDroppedDocuments()
                .DroppedDocumentCallback(DroppedResponseCallback));
        }

        private static void DroppedResponseCallback(BulkResponseItemBase droppedBulkResponse, PriceItem priceItem)
        {
            Console.WriteLine("failed indexing price {0} - reason {1}", priceItem, droppedBulkResponse.Error.Reason);
        }
    }

    public class PriceItem
    {

        public string Key { get; set; }
        public decimal Amount { get; set; }
        public DateTime FromDate { get; set; }
        public DateTime ToDate { get; set; }
    }
}
