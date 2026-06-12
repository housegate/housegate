load("@rules_img//img:image.bzl", "image_manifest")
load("@rules_img//img:load.bzl", "image_load")
load("@rules_img//img:push.bzl", "image_push")
load("@rules_multirun//:defs.bzl", "command", "multirun")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

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

def _registry_and_repository(registry_url, image_name):
    parts = registry_url.split("/", 1)
    if len(parts) != 2:
        fail("registry URL must include a repository path: %s" % registry_url)
    return parts[0], "%s/%s" % (parts[1], image_name)

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

    image_manifest(
        name = "image",
        base = base,
        entrypoint = [] if entrypoint == None else entrypoint,
        cmd = [] if cmd == None else cmd,
        layers = tars,
        env = env,
        platform = "//:linux_amd64",
        tags = [
            "manual",
        ],
        working_dir = workdir,
    )

    local_image = "sentio/%s:local" % image_name
    image_load(
        name = "image.load",
        image = ":image",
        tag = local_image,
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

    push_targets = []
    for registry_name, registry_url in REGISTRIES.items():
        push_name = "%s.push" % registry_name  # Construct rule name
        push_registry, push_repository = _registry_and_repository(registry_url, image_name)
        image_push(
            name = push_name,
            image = ":image",
            registry = push_registry,
            repository = push_repository,
            tag_file = "//:stamped",
            tags = [
                "manual",
            ],
        )
        push_targets.append(":%s" % push_name)

    multirun(
        name = "image.push",
        commands = push_targets,
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
