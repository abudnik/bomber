#include <elliptics/cppdef.h>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <csignal>

using namespace ioremap::elliptics;


struct CommandCodes
{
	const int code;
	const char *command;
};
const CommandCodes g_codes[] = {
	{0, "write"},
	{1, "plain_write"},
	{2, "read"},
	{3, "remove"},
	{4, "set_backend_readonly"},
	{5, "set_backend_writable"},
	{6, "start_defrag"},
	{7, "stat_monitor"},
	{8, "write_cache"}
};

static int MAX_KEY_VALUE = 10 * 1000 * 1000;
static size_t MAX_DATA_SIZE = 64 * 1024; // 64 Kb

struct helper
{
	static std::string rand_key() {
		int v = rand() % MAX_KEY_VALUE;
		return std::to_string(v);
	}

	static data_pointer rand_data() {
		std::ifstream rnd("/dev/urandom", std::ifstream::binary | std::fstream::in);
		if (!rnd)
			throw std::logic_error("couldn't open /dev/urandom");
		data_pointer ptr = data_pointer::allocate(MAX_DATA_SIZE);
		rnd.read(reinterpret_cast<char *>(ptr.data()), ptr.size());
		return ptr;
	}
};


class command
{
public:
	command(file_logger *log, const std::shared_ptr<session> &sess)
	: m_log(log),
	 m_sess(sess)
	{}

	command() = default;

	virtual ~command() {}

	virtual void exec() = 0;
	virtual void wait_result() = 0;

protected:
	file_logger *m_log;
	std::shared_ptr<session> m_sess;
};

typedef std::shared_ptr<command> command_ptr;


class command_repeater : public command
{
public:
	command_repeater(int times, command_ptr cmd)
	: m_times(times), m_cmd(cmd)
	{}

	virtual void exec() {
		for (int i = 0; i < m_times; ++i) {
			m_cmd->exec();
			m_cmd->wait_result();
		}
	}

	virtual void wait_result() {}

private:
	int m_times;
	command_ptr m_cmd;
};

class writer : public command
{
public:
	writer(file_logger *log, const std::shared_ptr<session> &sess)
	: command(log, sess)
	{}

	virtual void exec() {
		std::string key = helper::rand_key();
		data_pointer data = std::move(helper::rand_data());
		m_result = m_sess->write_data(key, data, 0, data.size());
	}

	virtual void wait_result() {
		if (m_result.is_valid())
			m_result.wait();
	}

private:
	async_write_result m_result;
};

class plain_writer : public command
{
public:
	plain_writer(file_logger *log, const std::shared_ptr<session> &sess)
	: command(log, sess)
	{}

	virtual void exec() {
		std::string key = helper::rand_key();
		data_pointer data = std::move(helper::rand_data());

		auto prepare_result = m_sess->write_prepare(key, data, 0, data.size());
		if (prepare_result.is_valid())
			prepare_result.wait();

		auto write_result = m_sess->write_plain(key, data, 0);
		if (write_result.is_valid())
			write_result.wait();

		m_result = m_sess->write_commit(key, data, 0, data.size());
	}

	virtual void wait_result() {
		if (m_result.is_valid())
			m_result.wait();
	}

private:
	async_write_result m_result;
};

class cache_writer : public command
{
public:
	cache_writer(file_logger *log, const std::shared_ptr<session> &sess)
	: command(log, sess)
	{}

	virtual void exec() {
		std::string key = helper::rand_key();
		data_pointer data = std::move(helper::rand_data());
		m_result = m_sess->write_cache(key, data, 123);
	}

	virtual void wait_result() {
		if (m_result.is_valid())
			m_result.wait();
	}

private:
	async_write_result m_result;
};

class reader : public command
{
public:
	reader(file_logger *log, const std::shared_ptr<session> &sess)
	: command(log, sess)
	{}

	virtual void exec() {
		auto key = helper::rand_key();
		auto lookup_result = m_sess->lookup(key);
		try {
			auto file_info = lookup_result.get_one().file_info();
			m_result = m_sess->read_data(key, 0, file_info->size);
		}
		catch(...) {}
	}

	virtual void wait_result() {
		if (m_result.is_valid())
			m_result.wait();
	}

private:
	async_read_result m_result;
};

class remover : public command
{
public:
	remover(file_logger *log, const std::shared_ptr<session> &sess)
	: command(log, sess)
	{}

	virtual void exec() {
		auto key = helper::rand_key();
		m_result = m_sess->remove(key);
	}

	virtual void wait_result() {
		if (m_result.is_valid())
			m_result.wait();
	}

private:
	async_remove_result m_result;
};

class backend_readonly_setter : public command
{
public:
	backend_readonly_setter(file_logger *log, const std::shared_ptr<session> &sess)
	: command(log, sess)
	{}

	virtual void exec() {
		return;
		auto routes = m_sess->get_routes();
		if (routes.empty()) return;
		const auto &route = routes[0];
		m_result = m_sess->make_readonly(route.addr, route.backend_id);
	}

	virtual void wait_result() {
		if (m_result.is_valid())
			m_result.wait();
	}

private:
	async_backend_control_result m_result;
};

class backend_writable_setter : public command
{
public:
	backend_writable_setter(file_logger *log, const std::shared_ptr<session> &sess)
	: command(log, sess)
	{}

	virtual void exec() {
		return;
		auto routes = m_sess->get_routes();
		if (routes.empty()) return;
		const auto &route = routes[0];
		m_result = m_sess->make_writable(route.addr, route.backend_id);
	}

	virtual void wait_result() {
		if (m_result.is_valid())
			m_result.wait();
	}

private:
	async_backend_control_result m_result;
};

class defrag_starter : public command
{
public:
	defrag_starter(file_logger *log, const std::shared_ptr<session> &sess)
	: command(log, sess)
	{}

	virtual void exec() {
		auto routes = m_sess->get_routes();
		if (routes.empty()) return;
		const auto &route = routes[0];
		m_result = m_sess->start_defrag(route.addr, route.backend_id);
	}

	virtual void wait_result() {
		if (m_result.is_valid())
			m_result.wait();
	}

private:
	async_backend_control_result m_result;
};

class monitor_stat_getter : public command
{
public:
	monitor_stat_getter(file_logger *log, const std::shared_ptr<session> &sess)
	: command(log, sess)
	{}

	virtual void exec() {
		m_result = m_sess->monitor_stat(-1); // all categories
	}

	virtual void wait_result() {
		if (m_result.is_valid())
			m_result.wait();
	}

private:
	async_monitor_stat_result m_result;
};


class actions
{
public:
	actions()
	: m_stopped(false)
	{}

	void add(const command_ptr &cmd) {
		m_commands.push_back(cmd);
	}

	void stop() {
		m_stopped = true;
	}

	void exec(int times) {
		if (times > 0) {
			for (int i = 0; i < times && !m_stopped; ++i) {
				do_exec();
			}
		} else {
			while (!m_stopped) {
				do_exec();
			}
		}
	}

private:
	void do_exec() {
		for (auto &cmd : m_commands) {
			cmd->exec();
		}

		for (auto &cmd : m_commands) {
			if (m_stopped)
				break;
			cmd->wait_result();
		}
	}

private:
	std::vector<command_ptr> m_commands;
	bool m_stopped;
};

typedef std::shared_ptr<actions> actions_ptr;

void thread_fun(actions_ptr act, int times) {
	act->exec(times);
}


class bomber
{
public:
	bomber()
	: m_log("/dev/stdout", DNET_LOG_DEBUG),
	 m_node(logger(m_log, blackhole::log::attributes_t()))
	{
	}

	void init(int argc, char *argv[]) {
		if (argc < 4)
			throw std::logic_error("usage: bomber <addr:port:family> <num_threads> <max_key_value> <max_data_size> <[command_name]* | 'all'>");

		BH_LOG(m_log, DNET_LOG_INFO, "creating addr remote: %s", argv[1])
			("source", "dnet_add_state");

		address addr = argv[1];
		m_num_threads = std::stoi(argv[2]);
		MAX_KEY_VALUE = std::stoi(argv[3]);
		MAX_DATA_SIZE = std::stoi(argv[4]);

		if (!strcmp(argv[5], "all")) {
			const size_t num_commands = sizeof(g_codes) / sizeof(g_codes[0]);
			for (size_t i = 0; i < num_commands; ++i) {
				m_commands.push_back(i);
			}
		} else {
			for (int i = 5; i < argc; ++i) {
				const auto it = std::find_if(std::begin(g_codes), std::end(g_codes),
						       [i, argv] (const CommandCodes &code) -> bool { return !strcmp(argv[i], code.command); } );

				if (it == std::end(g_codes))
					throw std::logic_error( "unknown command: " + std::string(argv[i]) );

				m_commands.push_back(it->code);
			}
		}

		BH_LOG(m_log, DNET_LOG_INFO, "connecting to remote: %s", argv[1])
			("source", "dnet_add_state");

		m_node.add_remote(addr);

		BH_LOG(m_log, DNET_LOG_INFO, "connected to remote: %s", argv[1])
			("source", "dnet_add_state");

		m_sess = std::make_shared<session>(m_node);
		m_sess->set_groups({1});
		m_sess->set_namespace("bomber");
		m_sess->set_exceptions_policy(session::no_exceptions);
	}

	void shutdown() {
		for (int i = 0; i < m_num_threads; ++i) {
			m_actions[i]->stop();
		}

		for (int i = 0; i < m_num_threads; ++i) {
			m_threads[i].join();
		}
	}

	void run() {
		m_actions.resize(m_num_threads);
		m_threads.resize(m_num_threads);
		for (int i = 0; i < m_num_threads; ++i) {
			m_actions[i] = prepare_random_actions();
			const int times = -1; // unlimited
			m_threads[i] = std::thread(thread_fun, m_actions[i], times);
		}
	}

private:
	actions_ptr prepare_random_actions() {
		actions_ptr act = std::make_shared<actions>();
		const int num_commands = 10;
		for (int i = 0; i < num_commands; ++i) {
			command_ptr cmd(generate_random_cmd());
			act->add(cmd);
		}
		return act;
	}

	command *generate_random_cmd() {
		const int i = rand() % m_commands.size();
		switch (m_commands[i]) {
			case 0: return new writer(&m_log, m_sess);
			case 1: return new plain_writer(&m_log, m_sess);
			case 2: return new reader(&m_log, m_sess);
			case 3: return new remover(&m_log, m_sess);
			case 4: return new backend_readonly_setter(&m_log, m_sess);
			case 5: return new backend_writable_setter(&m_log, m_sess);
			case 6: return new defrag_starter(&m_log, m_sess);
			case 7: return new monitor_stat_getter(&m_log, m_sess);
			case 8: return new cache_writer(&m_log, m_sess);
		}
		return nullptr;
	}

private:
	file_logger m_log;
	node m_node;
	std::shared_ptr<session> m_sess;
	int m_num_threads;
	std::vector<std::thread> m_threads;
	std::vector<actions_ptr> m_actions;
	std::vector<int> m_commands;
};


void wait_termination()
{
	sigset_t waitset;
	int sig;
	sigemptyset(&waitset);
	sigaddset(&waitset, SIGTERM);
	while (1)
	{
		int ret = sigwait(&waitset, &sig);
		if (ret == EINTR)
			continue;
		if (!ret)
			break;
		std::cerr << "main(): sigwait failed: " << strerror(ret) << std::endl;
	}
}

int main(int argc, char *argv[])
{
	srand(time(nullptr));
	try {
		bomber b;
		b.init(argc, argv);
		b.run();
		wait_termination();
		b.shutdown();
	}
	catch (const std::exception &ex) {
		std::cerr << "Caught exception: " << ex.what() << std::endl;
		return 1;
	}
	catch (...) {
		std::cerr << "caught unknown exception" << std::endl;
		return 1;
	}
	return 0;
}
