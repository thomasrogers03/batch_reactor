Gem::Specification.new do |s|
  s.name = 'batch_reactor'
  s.version = '0.1.2'
  s.license = 'Apache License 2.0'
  s.summary = 'A reactor pattern batching system for Ruby'
  s.description = %q{A reactor pattern batching system for Ruby.
It provides simple functionality for batching work in background thread.
Also adds simple but powerful work partitioning behaviour}
  s.authors = ['Thomas RM Rogers']
  s.email = 'thomasrogers03@gmail.com'
  s.files = Dir['{lib}/**/*.rb', 'bin/*', 'LICENSE.txt', '*.md']
  s.require_path = 'lib'
  s.homepage = 'https://www.github.com/thomasrogers03/batch_reactor'
  s.add_runtime_dependency 'thomas_utils', '~> 0.2.4'
end
