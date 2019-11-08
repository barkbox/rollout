require "rollout/version"
require "zlib"
require "set"
require "json"

class Rollout
  attr_accessor :options

  RAND_BASE = (2**32 - 1) / 100.0
  REDIS_SET_TOKEN = "__RS__".freeze

  # Some simple timing for common operations on medium sized sets -- doesn't need to
  # run as part of tests, but good to have handy in dev to check we're getting the
  # improvements we expect.
  #
  # creating user array
  # adding 25k members, redis
  # 16.880000   1.780000  18.660000 ( 18.681564)
  # adding 25k members, plain-old-strings
  # 201.690000  25.610000 227.300000 (239.508115)
  # checking 25k members, redis
  # 8.470000   0.910000   9.380000 (  9.411483)
  # checking 25k members, plain-old-strings
  # 231.760000  24.780000 256.540000 (259.218197)
  # deactiveate 25k members, redis
  # 16.360000   1.740000  18.100000 ( 18.120741)
  # deactivate 25k members, plain-old-strings
  # 249.480000  24.980000 274.460000 (286.155507)

  def self._benchmarks
    _feature_rs = Rollout::FeatureRS.new("bm_rs")
    feature_pos = Rollout::Feature.new("bm_pos")
    # need to save the pos feature explicitly so it's not recreated as the default
    $rollout.save(feature_pos)
    # this will sub in for a user array -- 25k users
    puts "creating user array"
    y = Array.new(25000).map { |_e| SecureRandom.uuid }
    puts "adding 25k members, redis"
    puts Benchmark.measure do
      y.map { |e| $rollout.activate_user(:bm_rs, e) } # 16.9 secs
    end
    puts "adding 25k members, plain-old-strings"
    puts Benchmark.measure do
      y.map { |e| $rollout.activate_user(:bm_pos, e) } # 198.5 secs
    end
    puts "checking 25k members, redis"
    puts Benchmark.measure do
      25000.times { $rollout.active?(:bm_rs, y.sample) }
    end
    puts "checking 25k members, plain-old-strings"
    puts Benchmark.measure do
      25000.times { $rollout.active?(:bm_pos, y.sample) }
    end
    puts "deactiveate 25k members, redis"
    puts Benchmark.measure do
      25000.times { $rollout.deactivate_user(:bm_rs, y.sample) }
    end
    puts "deactivate 25k members, plain-old-strings"
    puts Benchmark.measure do
      25000.times { $rollout.deactivate_user(:bm_pos, y.sample) }
    end
  end

  # With the change to support redis sets for membership, both the Feature class
  # and the Rollout class need to hold a reference to storage, so we'll break it
  # out into shared state

  class Storage
    @store = nil
    class << self
      attr_accessor :store
    end
  end

  # Redis set backed Feature class -- maintain the interface of the existing
  # Feature class, but store some data server-side in Redis. Don't require
  # local copy of server side data for membership operations. Don't overwrite
  # all server side user/membership data with changed local copy

  class FeatureRS
    attr_accessor :percentage, :data
    attr_reader :name, :options

    def initialize(name, string = nil, opts = {})
      @options = opts
      @name = name
      @groups = Storage.store.smembers(group_storage_key).map(&:to_sym)
      @groups = @groups.to_set if opts[:use_sets]
      if string
        raw_percentage, _raw_users, _raw_groups, raw_data = string.split('|', 4)
        # the last portion of the data string is a magic token that the factory uses to
        # correctly instantiate the right Feture type -- need to parse that out and can
        # throw away ( it's added back during serializtion )
        _tok_throwaway = raw_data.rpartition('|').last
        raw_data = raw_data.rpartition('|').first
        @percentage = raw_percentage.to_f
        @data = raw_data.nil? || raw_data.strip.empty? ? {} : JSON.parse(raw_data)
      else
        clear
      end
    end

    def groups
      @groups = Storage.store.smembers(group_storage_key).sort.map(&:to_sym)
      @groups = @groups.to_set if @options[:use_sets]
      @groups
    end

    def groups=(groups)
      Storage.store.del(group_storage_key)
      Storage.store.sadd(group_storage_key, groups) if !groups.empty?
    end

    def users
      @users = Storage.store.smembers(user_storage_key)
      @users = @users.to_set if @options[:use_sets]
      @users
    end

    def users=(new_users)
      Storage.store.del(user_storage_key)
      Storage.store.sadd(user_storage_key, new_users) if !groups.empty?
    end

    def user_storage_key
      "feature:#{@name}:users"
    end

    def group_storage_key
      "feature:#{@name}:groups"
    end

    def serialize
      "#{@percentage}|||#{serialize_data}|#{Rollout::REDIS_SET_TOKEN}"
    end

    def add_user(user)
      id = user_id(user)
      Storage.store.sadd(user_storage_key, id)
    end

    def remove_user(user)
      Storage.store.srem(user_storage_key, user_id(user))
    end

    def add_group(group)
      Storage.store.sadd(group_storage_key, group.to_sym)
    end

    def remove_group(group)
      Storage.store.srem(group_storage_key, group.to_sym)
    end

    def clear
      Storage.store.del(group_storage_key)
      Storage.store.del(user_storage_key)
      @percentage = 0
      @data = {}
    end

    def active?(rollout, user)
      if user
        id = user_id(user)
        user_in_percentage?(id) ||
          user_in_active_users?(id) ||
          user_in_active_group?(user, rollout)
      else
        @percentage == 100
      end
    end

    def user_in_active_users?(user)
      Storage.store.sismember(user_storage_key, user_id(user))
    end

    def to_hash
      {
        percentage: @percentage,
        groups: groups,
        users: users
      }
    end

    private

    def user_id(user)
      if user.is_a?(Integer) || user.is_a?(String)
        user.to_s
      else
        user.send(id_user_by).to_s
      end
    end

    def id_user_by
      @options[:id_user_by] || :id
    end

    def user_in_percentage?(user)
      Zlib.crc32(user_id_for_percentage(user)) < RAND_BASE * @percentage
    end

    def user_id_for_percentage(user)
      if @options[:randomize_percentage]
        user_id(user).to_s + @name.to_s
      else
        user_id(user)
      end
    end

    def user_in_active_group?(user, rollout)
      @groups.any? do |g|
        rollout.active_in_group?(g, user)
      end
    end

    def serialize_data
      return "" unless @data.is_a? Hash
      @data.to_json
    end
  end

  class Feature
    attr_accessor :groups, :users, :percentage, :data
    attr_reader :name, :options

    def initialize(name, string = nil, opts = {})
      @options = opts
      @name    = name

      if string
        raw_percentage, raw_users, raw_groups, raw_data = string.split('|', 4)
        @percentage = raw_percentage.to_f
        @users = users_from_string(raw_users)
        @groups = groups_from_string(raw_groups)
        @data = raw_data.nil? || raw_data.strip.empty? ? {} : JSON.parse(raw_data)
      else
        clear
      end
    end

    def serialize
      "#{@percentage}|#{@users.to_a.join(',')}|#{@groups.to_a.join(',')}|#{serialize_data}"
    end

    def add_user(user)
      id = user_id(user)
      @users << id unless @users.include?(id)
    end

    def remove_user(user)
      @users.delete(user_id(user))
    end

    def add_group(group)
      @groups << group.to_sym unless @groups.include?(group.to_sym)
    end

    def remove_group(group)
      @groups.delete(group.to_sym)
    end

    def clear
      @groups = groups_from_string("")
      @users = users_from_string("")
      @percentage = 0
      @data = {}
    end

    def active?(rollout, user)
      if user
        id = user_id(user)
        user_in_percentage?(id) ||
          user_in_active_users?(id) ||
          user_in_active_group?(user, rollout)
      else
        @percentage == 100
      end
    end

    def user_in_active_users?(user)
      @users.include?(user_id(user))
    end

    def to_hash
      {
        percentage: @percentage,
        groups: @groups,
        users: @users
      }
    end

    private

    def user_id(user)
      if user.is_a?(Integer) || user.is_a?(String)
        user.to_s
      else
        user.send(id_user_by).to_s
      end
    end

    def id_user_by
      @options[:id_user_by] || :id
    end

    def user_in_percentage?(user)
      Zlib.crc32(user_id_for_percentage(user)) < RAND_BASE * @percentage
    end

    def user_id_for_percentage(user)
      if @options[:randomize_percentage]
        user_id(user).to_s + @name.to_s
      else
        user_id(user)
      end
    end

    def user_in_active_group?(user, rollout)
      @groups.any? do |g|
        rollout.active_in_group?(g, user)
      end
    end

    def serialize_data
      return "" unless @data.is_a? Hash
      @data.to_json
    end

    def users_from_string(raw_users)
      users = (raw_users || "").split(",").map(&:to_s)
      if @options[:use_sets]
        users.to_set
      else
        users
      end
    end

    def groups_from_string(raw_groups)
      groups = (raw_groups || "").split(",").map(&:to_sym)
      if @options[:use_sets]
        groups.to_set
      else
        groups
      end
    end
  end

  def initialize(storage, opts = {})
    Storage::store = storage
    @storage = storage
    @options = opts
    @groups  = { all: lambda { |_user| true } }
  end

  def activate(feature, uid = nil, comment = nil)
    with_feature(feature) do |f|
      f.percentage = 100
      write_history(f, :update, uid, comment)
    end
  end

  def deactivate(feature, uid = nil, comment = nil)
    with_feature(feature) do |f|
      f.clear
      write_history(f, :clear, uid, comment)
    end
  end

  def add_history(feature, op, uid, comment)
    raise ArgumentError, 'op cannot contain space characters' if op.to_s.include? ' '

    with_feature(feature) do |f|
    write_history(f, op, uid, comment)
    end
  end

  def delete(feature)
    features = (@storage.get(features_key) || "").split(",")
    features.delete(feature.to_s)
    @storage.set(features_key, features.join(","))
    @storage.del(key(feature))
  end

  def set(feature, desired_state)
    with_feature(feature) do |f|
      if desired_state
        f.percentage = 100
      else
        f.clear
      end
    end
  end

  def activate_group(feature, group)
    with_feature(feature) do |f|
      f.add_group(group)
    end
  end

  def deactivate_group(feature, group)
    with_feature(feature) do |f|
      f.remove_group(group)
    end
  end

  def activate_user(feature, user)
    with_feature(feature) do |f|
      f.add_user(user)
    end
  end

  def deactivate_user(feature, user)
    with_feature(feature) do |f|
      f.remove_user(user)
    end
  end

  def activate_users(feature, users, uid = nil, comment =  nil)
    with_feature(feature) do |f|
      users.each { |user| f.add_user(user) }
      write_history(f, :activate_users, uid, comment)
    end
  end

  def deactivate_users(feature, users, uid  = nil, comment = nil)
    with_feature(feature) do |f|
      users.each { |user| f.remove_user(user) }
      write_history(f, :deactivate_users, uid, comment)
    end
  end

  def set_users(feature, users, uid = nil, comment = nil)
    with_feature(feature) do |f|
      f.users = []
      users.each { |user| f.add_user(user) }
      write_history(f, :set_users, uid, comment)
    end
  end

  def define_group(group, &block)
    @groups[group.to_sym] = block
  end

  def active?(feature, user = nil)
    feature = get(feature)
    feature.active?(self, user)
  end

  def user_in_active_users?(feature, user = nil)
    feature = get(feature)
    feature.user_in_active_users?(user)
  end

  def inactive?(feature, user = nil)
    !active?(feature, user)
  end

  def activate_percentage(feature, percentage, uid = nil, comment = nil)
    with_feature(feature) do |f|
      f.percentage = percentage
      write_history(f, :activate_percentage, uid, comment)
    end
  end

  def deactivate_percentage(feature, uid = nil, comment = nil)
    with_feature(feature) do |f|
      f.percentage = 0
      write_history(f, :deactivate_percentage, uid, comment)
    end
  end

  def active_in_group?(group, user)
    f = @groups[group.to_sym]
    f&.call(user)
  end

  def get(feature)
    string = @storage.get(key(feature))
    FeatureFactory.feature(feature, string, @options)
  end

  def set_feature_data(feature, data)
    with_feature(feature) do |f|
      f.data.merge!(data) if data.is_a? Hash
    end
  end

  def clear_feature_data(feature)
    with_feature(feature) do |f|
      f.data = {}
    end
  end

  def multi_get(*features)
    feature_keys = features.map { |feature| key(feature) }
    @storage.mget(*feature_keys).map.with_index { |string, index| FeatureFactory.feature(features[index], string, @options) }
  end

  def features
    (@storage.get(features_key) || "").split(",").map(&:to_sym)
  end

  def feature_states(user = nil)
    features.each_with_object({}) do |f, hash|
      hash[f] = active?(f, user)
    end
  end

  def active_features(user = nil)
    features.select do |f|
      active?(f, user)
    end
  end

  def clear!(uid = nil, comment = nil)
    features.each do |feature|
      with_feature(feature) do |f|
        f.clear
        write_history(f, :clear, uid, comment)
      end
      @storage.del(key(feature))
    end

    @storage.del(features_key)
  end

  def exists?(feature)
    @storage.exists(key(feature))
  end

  def save(feature)
    @storage.set(key(feature.name), feature.serialize)
    @storage.set(features_key, (features | [feature.name.to_sym]).join(","))
  end

  def get_most_recent_history(feature)
    entry = @storage.lindex(history_key(feature), 0)
    return nil if entry.nil?
    parse_history_record(entry)
  end

  def get_full_history(feature, max = -1)
    history = @storage.lrange(history_key(feature), 0, max > 0 ? max - 1 : max) || []
    history.map do |entry|
      parse_history_record(entry)
    end
  end

  def write_history(feature, op, uid, comment)
    if uid || comment
      @storage.lpush(
        history_key(feature.name),
        create_history_record(feature, op, uid, comment)
      )
    end
  end

  private

  def key(name)
    "feature:#{name}"
  end

  def features_key
    "feature:__features__"
  end

  def with_feature(feature)
    f = get(feature)
    yield(f)
    save(f)
  end

  def history_key(name)
    "feature:#{name}:history"
  end

  def create_history_record(feature, op, uid, comment)
    "#{op} #{uid} #{Time.now.to_i} #{feature.percentage} #{comment}"
  end

  def parse_history_record(str)
    op, uid, time_int, new_val, comment = str.split(' ', 5)
    {
      op: op.to_sym,
      uid: uid.to_i,
      timestamp: Time.at(time_int.to_i),
      new_value: new_val,
      comment: comment
    }
  end

  class FeatureFactory
    # Class will instantiate the correct type of feature class based on config with a
    # nod to backwards compatibility
    #
    # * If the store type is explicit in the config, use that value
    # * If there's no feature in Redis, create the default ( FeatureRS )
    # * If the feature exists, create based on presence of REDIS_SET_TOKEN -- this should allow you
    #   to deploy the gem when there's already existing Features in place and have then co-exist
    #   any migration needs to be handled manually for now
    #
    DEFAULT_FEATURE_STORE = Rollout::FeatureRS

    def self.feature(name, string, opts = {})
      feature_class(string, opts).new(name, string, opts)
    end

    def self.feature_class(string, opts = {})
      return Rollout::FeatureRS if opts && opts[:storage_type] == :STORE_RS
      return Rollout::Feature if opts && opts[:storage_type] == :STORE_POS
      return DEFAULT_FEATURE_STORE if string.nil?

      if string.include?(Rollout::REDIS_SET_TOKEN)
        return Rollout::FeatureRS
      else
        return Rollout::Feature
      end
    end
  end
end
