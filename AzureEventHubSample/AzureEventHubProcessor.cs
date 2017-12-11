//|---------------------------------------------------------------|
//|                         AZURE EVENT HUB                       |
//|---------------------------------------------------------------|
//|                       Developed by Wonde Tadesse              |
//|                             Copyright ©2017 - Present         |
//|---------------------------------------------------------------|
//|                         AZURE EVENT HUB                       |
//|---------------------------------------------------------------|
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.ServiceBus.Messaging;

namespace AzureEventHubSample
{
    public class AzureEventHubProcessor
    {
        #region Constant 

        private const string PARTITION_ID = "0"; // Define a specific partition to ease the publish/consume process
        
        #endregion
        
        #region Public Methods 

        /// <summary>
        /// Process Azure Event Hub
        /// </summary>
        /// <returns>Task object</returns>
        public async Task ProcessAzureEventHub()
        {
            Console.ForegroundColor = ConsoleColor.Green;
            EventHubClient eventHubClient = null;
            if (TryCreatingEventHubClient(out eventHubClient))
            {
                await Publish10MessagesToEventHub(eventHubClient);
                await Receive10EventHubMessage(eventHubClient);
                await eventHubClient.CloseAsync();
            }

        }

        #endregion

        #region Private Methods 

        /// <summary>
        /// Try creating Event Hub Client object
        /// </summary>
        /// <param name="eventHubClient">EventHubClient object</param>
        /// <returns>True/false</returns>
        private bool TryCreatingEventHubClient(out EventHubClient eventHubClient)
        {
            try
            {
                eventHubClient = EventHubClient.CreateFromConnectionString(ConfigurationManager.AppSettings.Get("EventHubConnectionString"), ConfigurationManager.AppSettings.Get("EventHubName"));

                Console.WriteLine("Event Hub Client successfully created !\n");
            }
            catch (Exception exception)
            {
                eventHubClient = null;
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error occurred !");
                Console.WriteLine(exception);
                return false;
            }
            return true;
        }

        /// <summary>
        /// Publish 10 Event Hub messages
        /// </summary>
        /// <param name="eventHubClient">EventHubClient object</param>
        /// <returns>Task object</returns>
        private async Task Publish10MessagesToEventHub(EventHubClient eventHubClient)
        {
            int maxMessagesToPublish = 10;
            int successMessageSentCounter = 0;
            Console.WriteLine($"About to publish [{maxMessagesToPublish}] message !\n");
            for (var counter = 0; counter < maxMessagesToPublish; counter++)
            {
                try
                {
                    var message = $"Sample event message {counter + 1}";
                    Console.WriteLine($"\tPublishing message content of [{message}]");
                    var eventData = new EventData(Encoding.UTF8.GetBytes(message));
                    EventHubSender eventHubSender = eventHubClient.CreatePartitionedSender(PARTITION_ID);
                    await eventHubSender.SendAsync(eventData);
                    Console.WriteLine($"\tPublishing message [{counter + 1}] is completed !\n");
                    successMessageSentCounter++;
                }
                catch (Exception exception)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Error occurred !");
                    Console.WriteLine(exception);
                }
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Delay 1 seconds for the publish the next message !\n");
                await Task.Delay(1000); // Delay 1 seconds for the publish the next message
            }
            if (successMessageSentCounter > 0)
            {
                Console.WriteLine($"[{successMessageSentCounter}] messages published successfully !\n");
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("No message published !");
            }
        }

        /// <summary>
        /// Receive 10 Event Hub messages
        /// </summary>
        /// <param name="eventHubClient">EventHubClient object</param>
        /// <returns>Task object</returns>
        private async Task Receive10EventHubMessage(EventHubClient eventHubClient)
        {
            int maxMessagesToReceive = 10;
            int successMessageReceiveCounter = 0;
            int recentPublishedMessageTimeIntervalInMinutes = 10; // Helps to get only the past 10 minutes published messages 
            Console.WriteLine($"About to receive [{maxMessagesToReceive}] message published the past [{recentPublishedMessageTimeIntervalInMinutes}] minutes !\n");
            try
            {
                EventHubConsumerGroup eventHubConsumerGroup = eventHubClient.GetConsumerGroup(ConfigurationManager.AppSettings.Get("EventHubConsumerGroup"));
                EventHubReceiver eventHubReceiver = eventHubConsumerGroup.CreateReceiver(PARTITION_ID, DateTime.UtcNow.AddMinutes(-recentPublishedMessageTimeIntervalInMinutes));
                IEnumerable<EventData> eventDatum = eventHubReceiver.Receive(maxMessagesToReceive);
                if (eventDatum == null)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Error occurred !");
                    Console.WriteLine("Event data is null !");
                }
                else
                {
                    foreach (EventData eventData in eventDatum)
                    {
                        successMessageReceiveCounter++;
                        string offset = eventData.Offset;
                        string content = Encoding.UTF8.GetString(eventData.GetBytes());
                        if (!string.IsNullOrWhiteSpace(offset) &&
                            !string.IsNullOrWhiteSpace(content))
                        {
                            Console.WriteLine($"\tReceived message offset - [{offset}]");
                            Console.WriteLine($"\tReceived message content - [{content}]\n");
                            Console.WriteLine("Delay 2 seconds to receive the next message !\n");

                            await Task.Delay(2000); // Delay 2 seconds to receive the next message
                        }
                        else
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine("Event data has no message content !");
                        }
                    }
                    if (successMessageReceiveCounter > 0)
                    {
                        Console.WriteLine($"[{successMessageReceiveCounter}] messages received successfully !\n");
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("No message received !");
                    }
                }
            }
            catch (Exception exception)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error occurred !");
                Console.WriteLine(exception);
            }
        }

        #endregion
    }
}
