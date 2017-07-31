Gem::Specification.new do |s|
  s.name          = 'logstash-input-elasticsearch_dynamic_aggregator'
  s.version       = '0.1.0'
  s.licenses      = ['Apache License (2.0)']
  s.summary       = 'LogStash input plugin that provides recurring aggregations based on dynamic datetime'
  s.homepage      = 'https://github.com/OmriBromberg/logstash-input-elasticsearch_dynamic_aggregator'
  s.authors       = ['Omri Bromberg']
  s.email         = 'obbromberg@gmail.com'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/logstash*','lib/logstash*/**/*','spec/**/*','vendor/jar-dependencies/**/*.jar','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency 'logstash-core-plugin-api', '~> 2.0'
  s.add_runtime_dependency 'jar-dependencies'
  s.add_runtime_dependency 'logstash-input-elasticsearch'
  s.add_runtime_dependency 'rufus-scheduler'
  s.add_development_dependency 'logstash-devutils', '>= 0.0.16'

  # Jar dependencies
  s.requirements << "jar 'com.github.omribromberg:elasticsearch-datemath', '0.4.1'"
end
