#include <MQTTClient.h>
#include <iostream>
#include <format>
#include <string>
#include <random>
#include <chrono>
#include <thread>
#include <csignal>
#include <numbers>
#include <cmath>
#include <memory>
#include <vector>
#include <array>

using namespace std::chrono_literals;

inline volatile bool g_running = true;

void signal_handler(int signal) noexcept {
    std::cout << std::format("\nShutdown signal received ({}). Terminating gracefully...\n", signal);
    g_running = false;
}

class TemperatureSimulator {
    mutable std::mt19937 generator_{std::random_device{}()};
    mutable std::normal_distribution<double> noise_{0.0, 0.3};
    
    double base_temp_;
    double amplitude_;
    mutable double current_temp_;
    
public:
    explicit TemperatureSimulator(double base = 23.0, double amp = 5.0) noexcept
        : base_temp_{base}, amplitude_{amp}, current_temp_{base} {}
    
    [[nodiscard]] double read_temperature() const noexcept {
        const auto now = std::chrono::system_clock::now();
        const auto time_t = std::chrono::system_clock::to_time_t(now);
        const auto* tm = std::localtime(&time_t);
        
        const double hour_radians = (tm->tm_hour - 4) * std::numbers::pi / 12.0;
        const double daily_variation = amplitude_ * std::sin(hour_radians);
        
        const double noise = noise_(generator_);
        current_temp_ += noise * 0.1;
        
        const double result = base_temp_ + daily_variation + current_temp_ * 0.1;
        return std::clamp(result, base_temp_ - amplitude_ - 2.0, base_temp_ + amplitude_ + 2.0);
    }
};

class MQTTTemperaturePublisher {
    MQTTClient client_;
    std::string device_id_;
    std::string zone_;
    std::string data_topic_;
    TemperatureSimulator simulator_;
    bool connected_{false};

public:
    MQTTTemperaturePublisher(const std::string& broker, const std::string& device_id, const std::string& zone)
        : device_id_{device_id}, zone_{zone}, 
          data_topic_{std::format("industrial/sensors/{}/{}/temperature", zone, device_id)},
          simulator_{20.0 + (std::hash<std::string>{}(device_id) % 60), 10.0} {
        
        const auto client_id = std::format("{}_publisher", device_id);
        MQTTClient_create(&client_, broker.c_str(), client_id.c_str(), 
                         MQTTCLIENT_PERSISTENCE_NONE, nullptr);
        
        std::cout << std::format("📡 MQTT Publisher initialized\n");
        std::cout << std::format("   Device ID: {}\n", device_id_);
        std::cout << std::format("   Data Topic: {}\n", data_topic_);
    }
    
    ~MQTTTemperaturePublisher() {
        disconnect();
        MQTTClient_destroy(&client_);
    }
    
    [[nodiscard]] bool connect() noexcept {
        try {
            std::cout << std::format("🔌 Connecting to MQTT broker...\n");
            
            MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
            conn_opts.keepAliveInterval = 60;
            conn_opts.cleansession = 1;
            
            int rc = MQTTClient_connect(client_, &conn_opts);
            if (rc == MQTTCLIENT_SUCCESS) {
                connected_ = true;
                std::cout << std::format("✅ Connected successfully!\n");
                return true;
            }
            
            std::cout << std::format("❌ Connection failed with code: {}\n", rc);
        } catch (...) {
            std::cout << std::format("❌ Connection exception occurred\n");
        }
        return false;
    }
    
    [[nodiscard]] bool publish_temperature() noexcept {
        if (!connected_) return false;
        
        const auto temperature = simulator_.read_temperature();
        const auto now = std::chrono::system_clock::now();
        
        const auto json_message = std::format(R"({{
    "device_id": "{}",
    "zone": "{}",
    "temperature": {:.2f},
    "unit": "celsius",
    "timestamp": "{}",
    "status": "active"
}})", device_id_, zone_, temperature, 
                      std::format("{:%Y-%m-%dT%H:%M:%SZ}", 
                                 std::chrono::floor<std::chrono::seconds>(now)));
        
        MQTTClient_message pubmsg = MQTTClient_message_initializer;
        pubmsg.payload = const_cast<char*>(json_message.c_str());
        pubmsg.payloadlen = json_message.length();
        pubmsg.qos = 1;
        pubmsg.retained = 1;
        
        MQTTClient_deliveryToken token;
        int rc = MQTTClient_publishMessage(client_, data_topic_.c_str(), &pubmsg, &token);
        
        if (rc == MQTTCLIENT_SUCCESS) {
            MQTTClient_waitForCompletion(client_, token, 5000);
            std::cout << std::format("📊 [{}] Zone: {} | Temperature: {:.2f}°C\n", 
                        device_id_, zone_, temperature);
            return true;
        }
        
        std::cout << std::format("❌ Publish failed with code: {}\n", rc);
        return false;
    }
    
    void disconnect() noexcept {
        if (connected_) {
            MQTTClient_disconnect(client_, 5000);
            connected_ = false;
            std::cout << std::format("🔌 Disconnected gracefully\n");
        }
    }
};

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    
    std::cout << std::format("🌡️ Industrial IoT Temperature Monitoring System\n");
    std::cout << std::format("================================================\n");
    
    const std::string broker_address = (argc > 1) ? argv[1] : "tcp://localhost:1883";
    const int device_count = (argc > 2) ? std::stoi(argv[2]) : 5;
    
    const std::array zones = {"production_floor", "warehouse", "cooling_system", "furnace_room", "quality_control"};
    
    std::cout << std::format("Configuration:\n");
    std::cout << std::format("  Broker: {}\n", broker_address);
    std::cout << std::format("  Devices: {}\n", device_count);
    std::cout << std::format("  Interval: 5 seconds\n\n");
    
    std::vector<std::unique_ptr<MQTTTemperaturePublisher>> publishers;
    
    for (int i = 0; i < device_count && i < zones.size(); ++i) {
        const auto device_id = std::format("temp_sensor_{:03d}", i + 1);
        publishers.emplace_back(std::make_unique<MQTTTemperaturePublisher>(broker_address, device_id, zones[i]));
        
        if (!publishers.back()->connect()) {
            std::cout << std::format("❌ Failed to connect device: {}\n", device_id);
            return 1;
        }
    }
    
    std::cout << std::format("🚀 Publishing started with {} devices. Press Ctrl+C to stop.\n", publishers.size());
    std::cout << std::format("📖 Subscribe: mosquitto_sub -h localhost -t \"industrial/sensors/+/+/temperature\"\n\n");
    
    int message_count = 0;
    while (g_running) {
        for (auto& publisher : publishers) {
            if (publisher->publish_temperature()) {
                ++message_count;
            }
        }
        
        for (int i = 0; i < 50 && g_running; ++i) {
            std::this_thread::sleep_for(100ms);
        }
    }
    
    std::cout << std::format("\n🛑 Shutting down all {} devices...\n", publishers.size());
    publishers.clear();
    
    std::cout << std::format("📈 Published {} messages total\n", message_count);
    std::cout << std::format("👋 Goodbye!\n");
    
    return 0;
}
