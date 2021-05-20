# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'yabeda/kafka/version'

Gem::Specification.new do |spec|
  spec.name          = 'yabeda-kafka'
  spec.version       = Yabeda::Kafka::VERSION
  spec.authors       = ['Dmitry Salahutdinov']
  spec.email         = ['dsalahutdinov@gmail.com']

  spec.summary       = 'Monitoring for ruby-kafka based on yabeda toolkit'
  spec.description   = 'Extends Yabeda metrics to collect ruby-kafka metrics'
  spec.homepage      = 'https://github.com/yabeda-rb/yabeda-kafka'
  spec.license       = 'MIT'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = 'https://github.com/yabeda-rb/yabeda-kafka'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_dependency 'activesupport', '>= 4.0'
  spec.add_dependency 'ruby-kafka'
  spec.add_dependency 'yabeda'

  # TODO: remove prometheus-client dependency for kafka/monitoring
  # https://github.com/zendesk/ruby-kafka/blob/master/lib/kafka/prometheus.rb
  spec.add_dependency 'prometheus-client'

  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'byebug'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec', '~> 3.0'
  spec.add_development_dependency 'rubocop', '~> 1.15'
end
