require "active_support"

require "ar_stats/version"
require "ar_stats/railtie"

ActiveSupport.on_load(:active_record) do
  require_relative "ar_stats/model"
  extend(ArStats::Model)
end
