using System;
using MediaDriver.Demo;

namespace MediaDriver
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var demo = new DemoSubscriber();
                demo.Run();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fatal Error: {ex}");
                Console.ReadLine();
            }
        }
    }
}
