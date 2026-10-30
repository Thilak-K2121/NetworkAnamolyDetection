
#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"

#include <fstream>
#include <iostream>
#include <string>

using namespace ns3;

// ==========================================
// TCP REPLAY CLIENT
// ==========================================

class ReplayClient : public Application
{
public:
    void Setup(Address address, std::string csvFile, Time interval)
    {
        m_peer = address;
        m_csvFile = csvFile;
        m_interval = interval;
    }

private:
    Ptr<Socket> m_socket;
    Address m_peer;
    std::string m_csvFile;
    Time m_interval;
    std::ifstream m_input;
    EventId m_sendEvent;

    void StartApplication() override
    {
        m_input.open(m_csvFile);

        if (!m_input.is_open())
        {
            NS_FATAL_ERROR("Cannot open CSV file: " << m_csvFile);
        }

        // Skip CSV header
        std::string header;
        std::getline(m_input, header);

        m_socket = Socket::CreateSocket(
            GetNode(),
            TcpSocketFactory::GetTypeId()
        );

        m_socket->SetConnectCallback(
            MakeCallback(&ReplayClient::ConnectionSucceeded, this),
            MakeCallback(&ReplayClient::ConnectionFailed, this)
        );

        std::cout << "[CLIENT] Connecting to server..." << std::endl;
        m_socket->Connect(m_peer);
    }

    void ConnectionSucceeded(Ptr<Socket> socket)
    {
        std::cout << "[CLIENT] TCP connection established." << std::endl;
        ScheduleNext();
    }

    void ConnectionFailed(Ptr<Socket> socket)
    {
        NS_FATAL_ERROR("TCP connection failed!");
    }

    void ScheduleNext()
    {
        m_sendEvent = Simulator::Schedule(
            m_interval,
            &ReplayClient::SendNextRecord,
            this
        );
    }

    void SendNextRecord()
    {
        std::string line;

        if (std::getline(m_input, line))
        {
            if (!line.empty() && line.back() == '\r')
                line.pop_back();

            line += "\n";

            Ptr<Packet> packet = Create<Packet>(
                reinterpret_cast<const uint8_t*>(line.data()),
                line.size()
            );

            int sent = m_socket->Send(packet);

            if (sent < 0)
            {
                std::cerr << "[CLIENT] Send failed. Retrying."
                          << std::endl;
                ScheduleNext();
                return;
            }

            // Schedule the next record after the configured interval.
            ScheduleNext();
        }
        else
        {
            std::cout << "[CLIENT] Dataset transmission completed."
                      << std::endl;

            std::string endMarker = "__END_OF_STREAM__\n";

            Ptr<Packet> packet = Create<Packet>(
                reinterpret_cast<const uint8_t*>(endMarker.data()),
                endMarker.size()
            );

            m_socket->Send(packet);
            m_socket->ShutdownSend();

            m_input.close();
        }
    }

    void StopApplication() override
    {
        if (m_sendEvent.IsPending())
            Simulator::Cancel(m_sendEvent);

        if (m_socket)
            m_socket->Close();

        if (m_input.is_open())
            m_input.close();
    }
};


// ==========================================
// TCP REPLAY SERVER
// ==========================================

class ReplayServer : public Application
{
public:
    void Setup(uint16_t port, std::string fifoPath)
    {
        m_port = port;
        m_fifoPath = fifoPath;
    }

private:
    Ptr<Socket> m_listenSocket;
    Ptr<Socket> m_connectionSocket;

    uint16_t m_port;
    std::string m_fifoPath;

    std::ofstream m_fifo;
    std::string m_buffer;

    void StartApplication() override
    {
        m_listenSocket = Socket::CreateSocket(
            GetNode(),
            TcpSocketFactory::GetTypeId()
        );

        InetSocketAddress localAddress(
            Ipv4Address::GetAny(),
            m_port
        );

        m_listenSocket->Bind(localAddress);
        m_listenSocket->Listen();

        m_listenSocket->SetAcceptCallback(
            MakeCallback(&ReplayServer::ConnectionRequest, this),
            MakeCallback(&ReplayServer::HandleAccept, this)
        );

        std::cout << "[SERVER] Listening on port "
                  << m_port << std::endl;

        // Python inference server must be started first.
        m_fifo.open(m_fifoPath);

        if (!m_fifo.is_open())
        {
            NS_FATAL_ERROR("Cannot open inference FIFO!");
        }

        std::cout << "[SERVER] Python inference bridge ready."
                  << std::endl;
    }

    bool ConnectionRequest(
        Ptr<Socket> socket,
        const Address& address
    )
    {
        return true;
    }

    void HandleAccept(
        Ptr<Socket> socket,
        const Address& address
    )
    {
        m_connectionSocket = socket;

        m_connectionSocket->SetRecvCallback(
            MakeCallback(&ReplayServer::HandleRead, this)
        );

        std::cout << "[SERVER] Client connected." << std::endl;
    }

    void HandleRead(Ptr<Socket> socket)
    {
        Ptr<Packet> packet;

        while ((packet = socket->Recv()))
        {
            if (packet->GetSize() == 0)
                break;

            uint32_t size = packet->GetSize();

            std::string data(size, '\0');

            packet->CopyData(
                reinterpret_cast<uint8_t*>(&data[0]),
                size
            );

            m_buffer += data;

            size_t newlinePosition;

            while (
                (newlinePosition = m_buffer.find('\n'))
                != std::string::npos
            )
            {
                std::string record =
                    m_buffer.substr(0, newlinePosition);

                m_buffer.erase(0, newlinePosition + 1);

                if (record == "__END_OF_STREAM__")
                {
                    std::cout << "[SERVER] All records received."
                              << std::endl;

                    m_fifo.flush();
                    m_fifo.close();

                    Simulator::Stop(
                        Simulator::Now() + MilliSeconds(100)
                    );

                    return;
                }

                if (!record.empty())
                {
                    m_fifo << record << "\n";
                    m_fifo.flush();
                }
            }
        }
    }

    void StopApplication() override
    {
        if (m_connectionSocket)
            m_connectionSocket->Close();

        if (m_listenSocket)
            m_listenSocket->Close();

        if (m_fifo.is_open())
            m_fifo.close();
    }
};


// ==========================================
// MAIN NS-3 SIMULATION
// ==========================================

int main(int argc, char* argv[])
{
    std::string csvFile =
        "/mnt/c/Users/user/Desktop/NWP-Final/dataset/dataset13.csv";

    std::string fifoPath =
        "/tmp/nwp-anomaly.pipe";

    // One record every 2 real-time seconds
    double intervalMs = 2000.0;

    CommandLine cmd;

    cmd.AddValue("csv", "Path to CSV dataset", csvFile);
    cmd.AddValue("fifo", "Path to Python inference FIFO", fifoPath);
    cmd.AddValue(
        "interval",
        "Delay between records in milliseconds",
        intervalMs
    );

    cmd.Parse(argc, argv);

    // IMPORTANT: Make NS-3 follow real wall-clock time.
    GlobalValue::Bind(
        "SimulatorImplementationType",
        StringValue("ns3::RealtimeSimulatorImpl")
    );

    // Create two simulated network nodes.
    NodeContainer nodes;
    nodes.Create(2);

    InternetStackHelper internet;
    internet.Install(nodes);

    // Point-to-point connection.
    PointToPointHelper pointToPoint;

    pointToPoint.SetDeviceAttribute(
        "DataRate",
        StringValue("10Mbps")
    );

    pointToPoint.SetChannelAttribute(
        "Delay",
        StringValue("2ms")
    );

    NetDeviceContainer devices =
        pointToPoint.Install(nodes);

    // Assign IP addresses.
    Ipv4AddressHelper address;

    address.SetBase(
        "10.1.1.0",
        "255.255.255.0"
    );

    Ipv4InterfaceContainer interfaces =
        address.Assign(devices);

    uint16_t port = 8080;

    // Install TCP server.
    Ptr<ReplayServer> server =
        CreateObject<ReplayServer>();

    server->Setup(port, fifoPath);

    nodes.Get(1)->AddApplication(server);

    server->SetStartTime(Seconds(0.0));
    server->SetStopTime(Seconds(10000.0));

    // Install TCP client.
    Ptr<ReplayClient> client =
        CreateObject<ReplayClient>();

    Address serverAddress(
        InetSocketAddress(
            interfaces.GetAddress(1),
            port
        )
    );

    client->Setup(
        serverAddress,
        csvFile,
        MilliSeconds(intervalMs)
    );

    nodes.Get(0)->AddApplication(client);

    client->SetStartTime(Seconds(1.0));
    client->SetStopTime(Seconds(10000.0));

    // Enable packet capture for Wireshark.
    pointToPoint.EnablePcap(
        "network-anomaly",
        devices.Get(0),
        true
    );

    std::cout << "\n====================================" << std::endl;
    std::cout << " NETWORK ANOMALY DETECTION" << std::endl;
    std::cout << "====================================" << std::endl;

    std::cout << "Client IP: " << interfaces.GetAddress(0) << std::endl;
    std::cout << "Server IP: " << interfaces.GetAddress(1) << std::endl;
    std::cout << "Streaming interval: "
              << intervalMs / 1000.0
              << " seconds per record" << std::endl;

    Simulator::Stop(Seconds(10000.0));

    Simulator::Run();
    Simulator::Destroy();

    std::cout << "[NS-3] Simulation finished." << std::endl;

    return 0;
}
