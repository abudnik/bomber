#ifndef __CONFIG_H
#define __CONFIG_H

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/lexical_cast.hpp>


struct config_params
{
	int num_threads;
	int max_key_value;
	size_t max_data_size;
	std::vector<std::string> remotes;
	std::vector<std::string> commands;
	size_t num_commands;
	int times;
	std::vector<int> groups;
};


template<typename T>
std::vector<T> convert_tokens(const std::vector<std::string> &tokens)
{
	std::vector<T> result;
	std::for_each(tokens.begin(), tokens.end(),
		      [&result] (const std::string &token) {
			      result.push_back(boost::lexical_cast<T>(token));
		      });
	return result;
}

template<>
std::vector<std::string> convert_tokens<std::string>(const std::vector<std::string> &tokens)
{
	return tokens;
}

class config
{
public:
	template<typename T>
	T get(const char *key) const
	{
		return m_ptree.get<T>(key);
	}

	template<typename T>
	std::vector<T> get_vector(const char *key) const
	{
		auto value = m_ptree.get<std::string>(key);
		std::istringstream ss(value);
		std::vector<std::string> tokens;
		std::copy(std::istream_iterator<std::string>(ss),
			  std::istream_iterator<std::string>(),
			  std::back_inserter(tokens));
		return convert_tokens<T>(tokens);
	}

	bool parse_config(const char *config_path)
	{
		std::ifstream file(config_path);
		if (!file.is_open()) {
			std::cerr << "config::parse_config: couldn't open " << config_path << std::endl;
			return false;
		}
		try {
			boost::property_tree::read_json(file, m_ptree);
		} catch (std::exception &e) {
			std::cerr << "config::parse_config: " << e.what() << std::endl;
			return false;
		}
		return true;
	}

private:
	boost::property_tree::ptree m_ptree;
};

#define GET_CONFIG_PARAM(param_name, param_type) \
	g_params.param_name = cfg.get<param_type>(#param_name);

#define GET_CONFIG_PARAM_VEC(param_name, param_type) \
	g_params.param_name = cfg.get_vector<param_type>(#param_name);

#endif
