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
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <ctime>
#include <string_view>

#ifdef _WIN32
#include <Windows.h>
#endif

using namespace std::chrono_literals;

// Custom transparent hasher for string containers
struct TransparentStringHash {
    using is_transparent = void;
    
    [[nodiscard]] std::size_t operator()(std::string_view sv) const noexcept {
        return std::hash<std::string_view>{}(sv);
    }
    
    [[nodiscard]] std::size_t operator()(const std::string& s) const noexcept {
        return std::hash<std::string>{}(s);
    }
    
    [[nodiscard]] std::size_t operator()(const char* s) const noexcept {
        return std::hash<std::string_view>{}(s);
    }
};

// Thread-safe system controller (fully compliant - no mutable globals)
class IoTSystemController {
private:
    std::atomic<bool> running_{true};
    mutable std::mutex log_mutex_;
    
    // Make constants static constexpr for better compliance
    static constexpr int DEFAULT_RECONNECT_DELAY = 1;
    static constexpr int MAX_RECONNECT_DELAY = 30;
    static constexpr int KEEP_ALIVE_INTERVAL = 60;
    static constexpr int CONNECTION_TIMEOUT = 10;
    static constexpr int PUBLISH_INTERVAL_SECONDS = 5;
    static constexpr int LOG_FREQUENCY = 10;
    static constexpr double SENSOR_WARNING_PROBABILITY = 0.01;
    
public:
    [[nodiscard]] bool is_running() const noexcept { 
        return running_.load(); 
    }
    
    void shutdown() noexcept { 
        running_.store(false); 
    }
    
    void log_message(const std::string& msg) const {
        std::scoped_lock lock(log_mutex_);
        auto now = std::chrono::system_clock::now();
        std::cout << std::format("[{:%Y-%m-%d %H:%M:%S}] {}\n", now, msg);
    }
    
    // Expose constants through member functions instead of global access
    [[nodiscard]] static constexpr int get_default_reconnect_delay() noexcept { 
        return DEFAULT_RECONNECT_DELAY; 
    }
    [[nodiscard]] static constexpr int get_max_reconnect_delay() noexcept { 
        return MAX_RECONNECT_DELAY; 
    }
    [[nodiscard]] static constexpr int get_keep_alive_interval() noexcept { 
        return KEEP_ALIVE_INTERVAL; 
    }
    [[nodiscard]] static constexpr int get_connection_timeout() noexcept { 
        return CONNECTION_TIMEOUT; 
    }
    [[nodiscard]] static constexpr int get_publish_interval_seconds() noexcept { 
        return PUBLISH_INTERVAL_SECONDS; 
    }
    [[nodiscard]] static constexpr int get_log_frequency() noexcept { 
        return LOG_FREQUENCY; 
    }
    [[nodiscard]] static constexpr double get_sensor_warning_probability() noexcept { 
        return SENSOR_WARNING_PROBABILITY; 
    }
    
    // Thread-safe singleton
    static IoTSystemController& instance() {
        static IoTSystemController controller;
        return controller;
    }
    
    // Prevent copying/moving
    IoTSystemController(const IoTSystemController&) = delete;
    IoTSystemController& operator=(const IoTSystemController&) = delete;
    IoTSystemController(IoTSystemController&&) = delete;
    IoTSystemController& operator=(IoTSystemController&&) = delete;
    
private:
    IoTSystemController() = default;
};

void signal_handler(int signal) noexcept {
    IoTSystemController::instance().log_message(
        std::format("Shutdown signal received ({}). Terminating gracefully...", signal));
    IoTSystemController::instance().shutdown();
}

// Enhanced temperature simulator with zone-specific profiles (REENTRANT VERSION)
class TemperatureSimulator {
private:
    struct ZoneProfile {
        double base_temp;
        double amplitude;
        double min_temp;
        double max_temp;
        double variance;
    };
    
    // Fixed: Use inline variable for proper header compatibility
    inline static const std::unordered_map<std::string, ZoneProfile, TransparentStringHash, std::equal_to<>> zone_profiles_{
        {"production_floor", {25.0, 5.0, 20.0, 35.0, 2.0}},
        {"warehouse", {22.0, 4.0, 15.0, 30.0, 1.5}},
        {"cooling_system", {5.0, 8.0, -10.0, 15.0, 3.0}},
        {"furnace_room", {60.0, 15.0, 40.0, 80.0, 5.0}},
        {"quality_control", {21.0, 2.0, 18.0, 25.0, 0.5}}
    };
    
    mutable std::mt19937 generator_{std::random_device{}()};
    mutable std::normal_distribution<double> noise_;
    mutable double current_temp_;
    ZoneProfile profile_;
    
    // Temperature smoothing constants
    static constexpr double TEMP_SMOOTHING_CURRENT = 0.7;
    static constexpr double TEMP_SMOOTHING_NEW = 0.3;
    
    // Fixed: Reduced nesting by extracting time conversion to separate functions
    [[nodiscard]] bool try_local_time_conversion(std::time_t time_val, int& hour) const noexcept {
#ifdef _WIN32
        if (std::tm local_time{}; ::localtime_s(&local_time, &time_val) == 0) {
            hour = local_time.tm_hour;
            return true;
        }
#else
        if (std::tm local_time{}; ::localtime_r(&time_val, &local_time) != nullptr) {
            hour = local_time.tm_hour;
            return true;
        }
#endif
        return false;
    }
    
    [[nodiscard]] bool try_utc_time_conversion(std::time_t time_val, int& hour) const noexcept {
#ifdef _WIN32
        if (std::tm utc_time{}; ::gmtime_s(&utc_time, &time_val) == 0) {
            hour = utc_time.tm_hour;
            return true;
        }
#else
        if (std::tm utc_time{}; ::gmtime_r(&time_val, &utc_time) != nullptr) {
            hour = utc_time.tm_hour;
            return true;
        }
#endif
        return false;
    }
    
    [[nodiscard]] int get_fallback_hour(const std::chrono::system_clock::time_point& now) const noexcept {
        auto hours = std::chrono::duration_cast<std::chrono::hours>(
            now.time_since_epoch()) % std::chrono::hours(24);
        return hours.count();
    }
    
    // Fixed: Completely flattened nesting structure - maximum 2 levels
    [[nodiscard]] int get_safe_hour(const std::chrono::system_clock::time_point& now) const noexcept {
        const auto time_val = std::chrono::system_clock::to_time_t(now);
        int hour = 0;
        
        // Try local time first
        if (try_local_time_conversion(time_val, hour)) {
            return hour;
        }
        
        // Try UTC time as fallback
        if (try_utc_time_conversion(time_val, hour)) {
            return hour;
        }
        
        // Ultimate fallback
        return get_fallback_hour(now);
    }
    
public:
    explicit TemperatureSimulator(const std::string& zone) noexcept {
        // Use init-statement to declare "it" inside the if statement
        if (const auto it = zone_profiles_.find(zone); it != zone_profiles_.end()) {
            profile_ = it->second;
        } else {
            // Default profile
            profile_ = {25.0, 5.0, 20.0, 30.0, 2.0};
        }
        
        current_temp_ = profile_.base_temp;
        noise_ = std::normal_distribution(0.0, profile_.variance);
    }
    
    [[nodiscard]] double read_temperature() const noexcept {
        const auto now = std::chrono::system_clock::now();
        
        // Fixed: Completely flat structure - no nested control flow
        const auto hour = get_safe_hour(now);
        const auto hour_radians = (hour - 4) * std::numbers::pi / 12.0;
        const auto daily_variation = profile_.amplitude * std::sin(hour_radians);
        
        // Add realistic noise with smoothing
        const auto noise = noise_(generator_);
        current_temp_ = TEMP_SMOOTHING_CURRENT * current_temp_ + 
                       TEMP_SMOOTHING_NEW * (profile_.base_temp + daily_variation + noise);
        
        // Clamp to zone limits
        return std::clamp(current_temp_, profile_.min_temp, profile_.max_temp);
    }
    
    [[nodiscard]] std::string get_status() const noexcept {
        // Simulate occasional sensor warnings using system constant
        std::uniform_real_distribution dist(0.0, 1.0);
        return (dist(generator_) < IoTSystemController::get_sensor_warning_probability()) 
               ? "warning" : "operational";
    }
};

// Enhanced MQTT publisher with automatic reconnection
class MQTTTemperaturePublisher {
private:
    MQTTClient client_;
    std::string device_id_;
    std::string zone_;
    std::string broker_uri_;
    std::string data_topic_;
    TemperatureSimulator simulator_;
    std::atomic<bool> connected_{false};
    std::atomic<int> reconnect_delay_{IoTSystemController::get_default_reconnect_delay()};
    std::atomic<bool> reconnecting_{false};
    std::jthread reconnect_thread_;
    
    // Helper function to reduce nesting in connection logic
    [[nodiscard]] bool attempt_mqtt_connection() noexcept {
        MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
        conn_opts.keepAliveInterval = IoTSystemController::get_keep_alive_interval();
        conn_opts.cleansession = 1;
        conn_opts.connectTimeout = IoTSystemController::get_connection_timeout();
        
        const auto rc = MQTTClient_connect(client_, &conn_opts);
        if (rc != MQTTCLIENT_SUCCESS) {
            IoTSystemController::instance().log_message(
                std::format("❌ [{}] Connection failed with code: {}", device_id_, rc));
            return false;
        }
        
        connected_ = true;
        reconnect_delay_ = IoTSystemController::get_default_reconnect_delay();
        IoTSystemController::instance().log_message(
            std::format("✅ [{}] Connected successfully!", device_id_));
        return true;
    }
    
    // Helper function to reduce nesting in publish logic
    [[nodiscard]] bool execute_mqtt_publish(const std::string& json_message) noexcept {
        auto mutable_message = json_message;
        
        MQTTClient_message pubmsg = MQTTClient_message_initializer;
        pubmsg.payload = mutable_message.data();
        pubmsg.payloadlen = mutable_message.length();
        pubmsg.qos = 1;
        pubmsg.retained = 1;
        
        MQTTClient_deliveryToken token;
        const auto rc = MQTTClient_publishMessage(client_, data_topic_.c_str(), &pubmsg, &token);
        
        if (rc != MQTTCLIENT_SUCCESS) {
            IoTSystemController::instance().log_message(
                std::format("❌ [{}] Publish failed with code: {}", device_id_, rc));
            return false;
        }
        
        MQTTClient_waitForCompletion(client_, token, 5000);
        return true;
    }
    
    // Helper function to handle sleep with interruption check
    void interruptible_sleep(int seconds, std::stop_token stop_token) const noexcept {
        for (int i = 0; i < seconds && !stop_token.stop_requested(); ++i) {
            std::this_thread::sleep_for(1s);
        }
    }
    
    // Helper function to check reconnection conditions
    [[nodiscard]] bool should_continue_reconnecting(std::stop_token stop_token) const noexcept {
        return !connected_ && 
               IoTSystemController::instance().is_running() && 
               !stop_token.stop_requested();
    }
    
    // Fixed: Completely flattened reconnection logic - maximum 2 levels of nesting
    void reconnection_worker(std::stop_token stop_token) noexcept {
        while (should_continue_reconnecting(stop_token)) {
            const auto delay = reconnect_delay_.load();
            IoTSystemController::instance().log_message(
                std::format("🔄 [{}] Attempting reconnection in {} seconds...", device_id_, delay));
            
            // Interruptible sleep
            interruptible_sleep(delay, stop_token);
            
            if (stop_token.stop_requested()) return;
            
            if (connect()) return; // Successfully connected, exit
            
            // Exponential backoff
            const auto new_delay = std::min(delay * 2, IoTSystemController::get_max_reconnect_delay());
            reconnect_delay_ = new_delay;
        }
        reconnecting_ = false;
    }

public:
    MQTTTemperaturePublisher(const std::string& broker, const std::string& device_id, const std::string& zone)
        : device_id_{device_id}, zone_{zone}, broker_uri_{broker},
          data_topic_{std::format("industrial/sensors/{}/{}/temperature", zone, device_id)},
          simulator_{zone} {
        
        const auto client_id = std::format("{}_publisher", device_id);
        MQTTClient_create(&client_, broker.c_str(), client_id.c_str(), 
                         MQTTCLIENT_PERSISTENCE_NONE, nullptr);
        
        IoTSystemController::instance().log_message(
            std::format("📡 MQTT Publisher initialized - Device: {}, Zone: {}", device_id_, zone_));
    }
    
    ~MQTTTemperaturePublisher() {
        disconnect();
        if (reconnect_thread_.joinable()) {
            reconnect_thread_.request_stop();
            reconnect_thread_.join();
        }
        MQTTClient_destroy(&client_);
    }
    
    [[nodiscard]] bool connect() noexcept {
        try {
            IoTSystemController::instance().log_message(
                std::format("🔌 [{}] Connecting to MQTT broker...", device_id_));
            return attempt_mqtt_connection();
        } catch (const std::system_error& e) {
            IoTSystemController::instance().log_message(
                std::format("❌ [{}] System error during connection: {}", device_id_, e.what()));
        } catch (const std::runtime_error& e) {
            IoTSystemController::instance().log_message(
                std::format("❌ [{}] Runtime error during connection: {}", device_id_, e.what()));
        } catch (...) {
            IoTSystemController::instance().log_message(
                std::format("❌ [{}] Unknown exception during connection", device_id_));
        }
        connected_ = false;
        return false;
    }
    
    void reconnect_with_backoff() noexcept {
        if (reconnecting_.exchange(true)) {
            return; // Already reconnecting
        }
        
        // Properly scoped thread
        reconnect_thread_ = std::jthread([this](std::stop_token stop_token) {
            reconnection_worker(stop_token);
        });
    }
    
    // Helper function to check connection status
    [[nodiscard]] bool is_mqtt_connected() const noexcept {
        return connected_ && MQTTClient_isConnected(client_);
    }
    
    // Helper function to handle failed publish
    void handle_publish_failure() noexcept {
        connected_ = false;
        if (!reconnecting_) {
            reconnect_with_backoff();
        }
    }
    
    // Fixed: Flattened publish logic - maximum 2 levels of nesting
    [[nodiscard]] bool publish_temperature() noexcept {
        // Early return if not connected
        if (!is_mqtt_connected()) {
            handle_publish_failure();
            return false;
        }
        
        try {
            // Generate sensor data
            const auto temperature = simulator_.read_temperature();
            const auto status = simulator_.get_status();
            const auto now = std::chrono::system_clock::now();
            const auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()).count();
            
            // Create JSON message
            const auto json_message = std::format(R"({{
    "device_id": "{}",
    "zone": "{}",
    "temperature": {:.2f},
    "unit": "celsius",
    "timestamp": {},
    "status": "{}"
}})", device_id_, zone_, temperature, timestamp, status);
            
            // Attempt to publish
            if (!execute_mqtt_publish(json_message)) {
                connected_ = false;
                return false;
            }
            
            // Log periodically
            static thread_local int log_counter = 0;
            if (++log_counter % IoTSystemController::get_log_frequency() == 0) {
                IoTSystemController::instance().log_message(
                    std::format("📊 [{}] Zone: {} | Temperature: {:.2f}°C | Status: {}", 
                              device_id_, zone_, temperature, status));
            }
            return true;
            
        } catch (const std::system_error& e) {
            IoTSystemController::instance().log_message(
                std::format("❌ [{}] System error during publish: {}", device_id_, e.what()));
        } catch (const std::runtime_error& e) {
            IoTSystemController::instance().log_message(
                std::format("❌ [{}] Runtime error during publish: {}", device_id_, e.what()));
        } catch (...) {
            IoTSystemController::instance().log_message(
                std::format("❌ [{}] Unknown exception during publish", device_id_));
        }
        connected_ = false;
        return false;
    }
    
    void disconnect() noexcept {
        if (connected_ && MQTTClient_isConnected(client_)) {
            MQTTClient_disconnect(client_, 5000);
            connected_ = false;
            IoTSystemController::instance().log_message(
                std::format("🔌 [{}] Disconnected gracefully", device_id_));
        }
    }
    
    [[nodiscard]] bool is_connected() const noexcept {
        return connected_ && MQTTClient_isConnected(client_);
    }
};

// Configuration parser
struct Config {
    std::string broker_uri = "tcp://localhost:1883";
    int device_count = 5;
    
    // Validation constants
    static constexpr int MIN_DEVICE_COUNT = 1;
    static constexpr int MAX_DEVICE_COUNT = 100;
    
    bool parse(int argc, char* argv[]) {
        if (argc > 1) {
            broker_uri = argv[1];
        }
        if (argc > 2) {
            try {
                device_count = std::stoi(argv[2]);
                if (device_count < MIN_DEVICE_COUNT || device_count > MAX_DEVICE_COUNT) {
                    std::cerr << std::format("Device count must be between {} and {}\n", 
                                           MIN_DEVICE_COUNT, MAX_DEVICE_COUNT);
                    return false;
                }
            } catch (const std::invalid_argument& e) {
                std::cerr << std::format("Invalid device count format: {}\n", e.what());
                return false;
            } catch (const std::out_of_range& e) {
                std::cerr << std::format("Device count out of range: {}\n", e.what());
                return false;
            }
        }
        return true;
    }
};

int main(int argc, char* argv[]) {
    // Parse configuration
    Config config;
    if (!config.parse(argc, argv)) {
        return 1;
    }
    
    // Set up signal handlers
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    
    // Use const reference
    const auto& controller = IoTSystemController::instance();
    
    controller.log_message("🌡️ Industrial IoT Temperature Monitoring System");
    controller.log_message("================================================");
    controller.log_message(std::format("Broker: {}", config.broker_uri));
    controller.log_message(std::format("Devices: {}", config.device_count));
    controller.log_message(std::format("Interval: {} seconds", IoTSystemController::get_publish_interval_seconds()));
    controller.log_message("Press Ctrl+C to shut down gracefully");
    controller.log_message("-------------------------------------------");
    
    // Zone configuration - const and local to main
    static constexpr auto zones = std::array{"production_floor", "warehouse", "cooling_system", "furnace_room", "quality_control"};
    auto publishers = std::vector<std::unique_ptr<MQTTTemperaturePublisher>>{};
    
    // Initialize publishers - flattened loop structure
    for (int i = 0; i < config.device_count && i < zones.size(); ++i) {
        const auto device_id = std::format("TEMP_{:03d}", i + 1);
        const auto& zone = zones[i % zones.size()];
        
        publishers.emplace_back(std::make_unique<MQTTTemperaturePublisher>(
            config.broker_uri, device_id, zone));
        
        if (!publishers.back()->connect()) {
            controller.log_message(std::format("❌ Failed to connect device: {}", device_id));
            // Continue with other devices instead of exiting
        }
    }
    
    controller.log_message(std::format("🚀 Publishing started with {} devices", publishers.size()));
    controller.log_message("📖 Subscribe: mosquitto_sub -h localhost -t \"industrial/sensors/+/+/temperature\"");
    
    // Main publishing loop - flattened structure
    int message_count = 0;
    auto last_publish = std::chrono::steady_clock::now();
    const auto publish_interval = std::chrono::seconds(IoTSystemController::get_publish_interval_seconds());
    
    while (controller.is_running()) {
        // Use init-statement to declare "now" inside the if statement
        if (const auto now = std::chrono::steady_clock::now(); now - last_publish >= publish_interval) {
            int successful = 0;
            
            // Publish from all sensors - single level loop
            for (const auto& publisher : publishers) {
                if (publisher->publish_temperature()) {
                    ++successful;
                    ++message_count;
                }
            }
            
            // Report partial failures
            if (successful < publishers.size()) {
                controller.log_message(std::format("⚠️ Published {}/{} readings successfully", 
                                                 successful, publishers.size()));
            }
            
            last_publish = now;
        }
        
        // Small sleep to prevent busy waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // Graceful shutdown
    controller.log_message(std::format("🛑 Shutting down all {} devices...", publishers.size()));
    publishers.clear(); // Triggers destructors for clean disconnect
    
    controller.log_message(std::format("📈 Published {} messages total", message_count));
    controller.log_message("👋 Industrial IoT Temperature Monitor stopped");
    
    return 0;
}
