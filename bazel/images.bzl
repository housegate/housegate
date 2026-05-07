load("@aspect_bazel_lib//lib:transitions.bzl", "platform_transition_filegroup")
load("@rules_multirun//:defs.bzl", "command", "multirun")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_image_index", "oci_load", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load(":transition.bzl", "multi_arch")

REGISTRIES = {
    "gar": "us-west1-docker.pkg.dev/sentio-352722/sentio",
    "harbor": "docker.sentioxyz.co/sentio",
}

all_dev_images = {
    "housegate": "//cmd:housegate",
}

all_images = all_dev_images
# | {
#    "driver_race": "//driver/cmd:image.push_race",
#}

package_to_image = {v: k for k, v in all_images.items()}

def create_go_image(binary, base = "@go_base_image", env = {}, image_name = ""):
    raw_binary = binary[1:]
    basedir = "/app/" + native.package_name() + "/" + raw_binary + "_/"
    entrypoint = [basedir + raw_binary]
    workdir = basedir + raw_binary + ".runfiles/_main"

    pkg_tar(
        name = "tar",
        srcs = [binary],
        include_runfiles = True,
        package_dir = "/app",
        strip_prefix = "/",
        tags = [
            "manual",
        ],
    )
    create_image_with_tar(base = base, workdir = workdir, cmd = None, env = env, entrypoint = entrypoint, tars = [":tar"], image_name = image_name)

def create_image_with_tar(tars, base = "@go_base_image", entrypoint = None, cmd = [], workdir = "/", env = {}, image_name = ""):
    package_label = "//" + native.package_name()
    if image_name == "":
        image_name = package_to_image[package_label]
    elif package_label not in package_to_image:
        print("Manual image '%s' in '%s' is not going to be in push all procedure, create an entry in images.bzl to push it" % (image_name, package_label))

    oci_image(
        workdir = workdir,
        name = "image",
        base = base,
        entrypoint = entrypoint,
        cmd = cmd,
        tars = tars,
        env = env,
        tags = [
            "manual",
        ],
    )

    platform_transition_filegroup(
        name = "platform_image",
        srcs = [":image"],
        target_platform = "//:linux_amd64",
        tags = [
            "manual",
        ],
    )

    local_image = "sentio/%s:local" % image_name
    oci_load(
        name = "image.load",
        image = ":platform_image",
        repo_tags = [local_image],
        tags = [
            "manual",
        ],
    )

    command(
        name = "image.run_only",
        command = "//bazel:docker",
        arguments = ["run", local_image],
        tags = [
            "manual",
        ],
    )
    multirun(
        name = "image.run",
        commands = [
            ":image.load",
            ":image.run_only",
        ],
        tags = [
            "manual",
        ],
    )

    #    multi_arch(
    #        name = "images",
    #        image = ":image",
    #        platforms = [
    #            #    "//:linux_arm64",
    #            "//:linux_amd64",
    #        ],
    #        tags = [
    #            "manual",
    #        ],
    #    )
    #
    #    oci_image_index(
    #        name = "image_index",
    #        images = [
    #            ":images",
    #        ],
    #        tags = [
    #            "manual",
    #        ],
    #    )

    oci_push_targets = []
    for registry_name, registry_url in REGISTRIES.items():
        oci_push_name = "%s.push" % registry_name  # Construct rule name
        oci_push(
            name = oci_push_name,
            image = ":platform_image",
            remote_tags = "//:stamped",
            repository = "%s/%s" % (registry_url, image_name),
            tags = [
                "manual",
            ],
        )
        oci_push_targets.append(":%s" % oci_push_name)

    multirun(
        name = "image.push",
        commands = oci_push_targets,
        tags = ["manual"],
        visibility = ["//visibility:public"],
    )

def create_push_commands(images):
    for key, value in images.items():
        command(
            name = "push_" + key,
            command = value + ":image.push",
            visibility = ["//visibility:public"],
            tags = [
                "manual",
            ],
        )
