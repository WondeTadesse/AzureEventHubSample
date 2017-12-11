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
using System.Text;

using System.Threading;
using System.Threading.Tasks;

namespace AzureEventHubSample
{
    class Program
    {

        static void Main(string[] args)
        {
            new AzureEventHubProcessor().ProcessAzureEventHub().GetAwaiter().GetResult();
            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

    }
}
