# frozen_string_literal: true

require "rubygems"

module HomebrewFormulaUpdater
  RELEASE_TAG = /\Av\d+\.\d+\.\d+\z/
  SHA256 = /\A[0-9a-f]{64}\z/

  module_function

  def update(path:, tag:, darwin_sha:, linux_sha:)
    raise ArgumentError, "invalid release tag: #{tag}" unless RELEASE_TAG.match?(tag)
    checksums = {
      "darwin-arm64" => darwin_sha,
      "linux-amd64" => linux_sha,
    }
    checksums.each do |platform, checksum|
      raise ArgumentError, "invalid #{platform} checksum" unless SHA256.match?(checksum)
    end

    version = tag.delete_prefix("v")
    body = File.read(path)
    original = body.dup
    release_versions = body.scan(
      %r{releases/download/v(\d+\.\d+\.\d+)/housegate-v\1-(?:darwin-arm64|linux-amd64)},
    ).flatten
    unless release_versions.length == 2 && release_versions.uniq.length == 1
      raise ArgumentError, "expected one shared version across two release URLs"
    end

    old_version = release_versions.first
    explicit_version = body[/^  version "([^"]+)"$/, 1]
    if explicit_version && explicit_version != old_version
      raise ArgumentError, "version stanza #{explicit_version} does not match release URLs #{old_version}"
    end
    if Gem::Version.new(version) < Gem::Version.new(old_version)
      raise ArgumentError, "refusing downgrade #{old_version} -> #{version}"
    end

    old_reference = "v#{old_version}"
    escaped_old_version = Regexp.escape(old_version)
    required_references = {
      "darwin URL reference" => %r{releases/download/v#{escaped_old_version}/housegate-v#{escaped_old_version}-darwin-arm64},
      "linux URL reference" => %r{releases/download/v#{escaped_old_version}/housegate-v#{escaped_old_version}-linux-amd64},
      "darwin install reference" => /bin\.install "housegate-v#{escaped_old_version}-darwin-arm64"/,
      "linux install reference" => /bin\.install "housegate-v#{escaped_old_version}-linux-amd64"/,
      "version test reference" => /assert_match "housegate v#{escaped_old_version}", shell_output/,
    }
    required_references.each do |name, pattern|
      count = body.scan(pattern).length
      raise ArgumentError, "expected one #{name}, found #{count}" unless count == 1
    end

    body.sub!(/^  version "#{Regexp.escape(old_version)}"\n/, "") if explicit_version
    body.gsub!(old_reference, "v#{version}")

    checksums.each do |platform, checksum|
      asset = "housegate-v#{version}-#{platform}"
      pattern = /(^\s+url ".*\/#{Regexp.escape(asset)}"\n\s+sha256 ")[0-9a-f]{64}("$)/
      raise ArgumentError, "#{platform} stanza not found" unless body.scan(pattern).length == 1

      body.sub!(pattern, "\\1#{checksum}\\2")
    end

    return :unchanged if body == original

    File.write(path, body)
    :updated
  end
end

if $PROGRAM_NAME == __FILE__
  unless ARGV.length == 4
    warn "usage: #{$PROGRAM_NAME} FORMULA_PATH TAG DARWIN_SHA256 LINUX_SHA256"
    exit 2
  end

  path, tag, darwin_sha, linux_sha = ARGV
  begin
    result = HomebrewFormulaUpdater.update(
      path: path,
      tag: tag,
      darwin_sha: darwin_sha,
      linux_sha: linux_sha,
    )
    puts "#{result} #{path} to #{tag}"
  rescue ArgumentError, SystemCallError => e
    warn "error: #{e.message}"
    exit 1
  end
end
