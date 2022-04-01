using RabbitMQ.Client;
using System.Text;

var factory = new ConnectionFactory
{
    HostName = "localhost",
    UserName = "merlinstgo",
    Password = "MerlinStgo2022"
};

using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{
    channel.QueueDeclare("MSCodeQueue", false, false, false, null);

    var message = "Bienvenido a sistema de colas de MS Code";
    var body = Encoding.UTF8.GetBytes(message);

    channel.BasicPublish("", "MSCodeQueue", null, body);

    Console.WriteLine("El mensaje fue enviado {0}", message);
}

Console.WriteLine("Presiona [enter] para salir de la aplicación");
Console.ReadLine();