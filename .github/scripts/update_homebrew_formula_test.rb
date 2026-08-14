# frozen_string_literal: true

require "minitest/autorun"
require "open3"
require "rbconfig"
require "tmpdir"

require_relative "update_homebrew_formula"

class UpdateHomebrewFormulaTest < Minitest::Test
  SCRIPT = File.expand_path("update_homebrew_formula.rb", __dir__)

  FORMULA = <<~RUBY
    class Housegate < Formula
      desc "ClickHouse native TCP proxy"
      homepage "https://github.com/housegate/housegate"
      version "0.3.0"
      license "Apache-2.0"

      if OS.mac? && Hardware::CPU.arm?
        url "https://github.com/housegate/housegate/releases/download/v0.3.0/housegate-v0.3.0-darwin-arm64"
        sha256 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      elsif OS.linux? && Hardware::CPU.intel?
        url "https://github.com/housegate/housegate/releases/download/v0.3.0/housegate-v0.3.0-linux-amd64"
        sha256 "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      end

      def install
        if OS.mac?
          bin.install "housegate-v0.3.0-darwin-arm64" => "housegate"
        else
          bin.install "housegate-v0.3.0-linux-amd64" => "housegate"
        end
      end

      test do
        assert_match "housegate v0.3.0", shell_output("\#{bin}/housegate --version")
      end
    end
  RUBY
  FORMULA_WITHOUT_VERSION = FORMULA.sub(/^  version ".*"\n/, "")

  def test_updates_every_versioned_reference_and_platform_checksum
    with_formula do |path|
      HomebrewFormulaUpdater.update(
        path: path,
        tag: "v0.6.0",
        darwin_sha: "c" * 64,
        linux_sha: "d" * 64,
      )

      body = File.read(path)
      refute_match(/^  version /, body)
      assert_equal 7, body.scan("v0.6.0").length
      refute_includes body, "v0.3.0"
      assert_match(/darwin-arm64"\n    sha256 "#{"c" * 64}"/, body)
      assert_match(/linux-amd64"\n    sha256 "#{"d" * 64}"/, body)
    end
  end

  def test_updates_formula_without_explicit_version
    with_formula(FORMULA_WITHOUT_VERSION) do |path|
      HomebrewFormulaUpdater.update(
        path: path,
        tag: "v0.6.0",
        darwin_sha: "c" * 64,
        linux_sha: "d" * 64,
      )

      body = File.read(path)
      refute_match(/^  version /, body)
      assert_equal 7, body.scan("v0.6.0").length
      refute_includes body, "v0.3.0"
    end
  end

  def test_removes_redundant_explicit_version
    with_formula do |path|
      HomebrewFormulaUpdater.update(
        path: path,
        tag: "v0.6.0",
        darwin_sha: "c" * 64,
        linux_sha: "d" * 64,
      )

      refute_match(/^  version /, File.read(path))
    end
  end

  def test_rejects_downgrades_without_changing_the_formula
    with_formula(FORMULA_WITHOUT_VERSION) do |path|
      error = assert_raises(ArgumentError) do
        HomebrewFormulaUpdater.update(
          path: path,
          tag: "v0.2.0",
          darwin_sha: "c" * 64,
          linux_sha: "d" * 64,
        )
      end

      assert_match "refusing downgrade 0.3.0 -> 0.2.0", error.message
      assert_equal FORMULA_WITHOUT_VERSION, File.read(path)
    end
  end

  def test_rejects_non_release_tags
    with_formula do |path|
      error = assert_raises(ArgumentError) do
        HomebrewFormulaUpdater.update(
          path: path,
          tag: "v0.6.0-rc1",
          darwin_sha: "c" * 64,
          linux_sha: "d" * 64,
        )
      end

      assert_match "invalid release tag", error.message
      assert_equal FORMULA, File.read(path)
    end
  end

  def test_rejects_invalid_checksums
    with_formula do |path|
      error = assert_raises(ArgumentError) do
        HomebrewFormulaUpdater.update(
          path: path,
          tag: "v0.6.0",
          darwin_sha: "not-a-sha",
          linux_sha: "d" * 64,
        )
      end

      assert_match "invalid darwin-arm64 checksum", error.message
      assert_equal FORMULA, File.read(path)
    end
  end

  def test_rejects_unexpected_formula_structure
    drifted = FORMULA.sub(
      'bin.install "housegate-v0.3.0-linux-amd64" => "housegate"',
      'bin.install "housegate-linux-amd64" => "housegate"',
    )

    with_formula(drifted) do |path|
      error = assert_raises(ArgumentError) do
        HomebrewFormulaUpdater.update(
          path: path,
          tag: "v0.6.0",
          darwin_sha: "c" * 64,
          linux_sha: "d" * 64,
        )
      end

      assert_match "expected one linux install reference", error.message
      assert_equal drifted, File.read(path)
    end
  end

  def test_reports_an_idempotent_retry_as_unchanged
    with_formula do |path|
      first = HomebrewFormulaUpdater.update(
        path: path,
        tag: "v0.6.0",
        darwin_sha: "c" * 64,
        linux_sha: "d" * 64,
      )
      first_body = File.read(path)
      second = HomebrewFormulaUpdater.update(
        path: path,
        tag: "v0.6.0",
        darwin_sha: "c" * 64,
        linux_sha: "d" * 64,
      )

      assert_equal :updated, first
      assert_equal :unchanged, second
      assert_equal first_body, File.read(path)
    end
  end

  def test_cli_updates_the_requested_formula
    with_formula do |path|
      stdout, stderr, status = Open3.capture3(
        RbConfig.ruby,
        SCRIPT,
        path,
        "v0.6.0",
        "c" * 64,
        "d" * 64,
      )

      assert status.success?, stderr
      assert_equal "updated #{path} to v0.6.0\n", stdout
      assert_empty stderr
      refute_match(/^  version /, File.read(path))
    end
  end

  private

  def with_formula(body = FORMULA)
    Dir.mktmpdir do |dir|
      path = File.join(dir, "housegate.rb")
      File.write(path, body)
      yield path
    end
  end
end
