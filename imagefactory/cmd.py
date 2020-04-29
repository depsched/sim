import os
import glob

from .utils import cmd, push_to_ecr, login_ecr, create_ecr_repository
from .wren_runtimes import runtimes, pkg_sets

_dir_path = os.path.dirname(os.path.realpath(__file__))
_dockerfile_path = _dir_path + "/dockerfiles/*"
ignore_image = {}
ecr_image = []


def build_runtimes():
    dockerfiles = glob.glob(_dockerfile_path)
    for f in dockerfiles:
        image_name = f.split("/")[-1]
        if image_name in ignore_image:
            continue
        cmd("cd {}; docker build . --tag {}".format(f, image_name))
        create_ecr_repository(image_name)
        push_to_ecr(image_name)
        ecr_image.append(image_name)
    print("uploaded: ", ecr_image)


def create_dockerfiles():
    cmd("rm -rf {}/dockerfiles/*".format(_dir_path))
    for runtime, lib in runtimes.items():
        runtime = "wren-" + runtime
        new_file = "{}/dockerfiles/{}/Dockerfile".format(_dir_path, runtime)
        conda_install = lib["packages"]["conda_install"]
        pip_install = lib["packages"]["pip_install"]
        pip_upgrade = lib["packages"]["pip_upgrade"]

        cmd("mkdir {}/dockerfiles/{}".format(_dir_path, runtime))
        cmd("cp {}/BaseDockerfile ".format(_dir_path) + new_file)
        with open(new_file, "a") as f:
            add_cmds = ["RUN pip install " + " ".join(pip_install),
                        "RUN pip install " + " ".join(conda_install),
                        "RUN pip install " + " ".join(pip_upgrade),]
            f.write("\n".join(add_cmds))
            f.write("\nADD ./handler.py /handler.py")
            f.write("\nCMD [\"python3\", \"/handler.py\"]")
        cmd("cp {}/handler.py {}".format(_dir_path, _dir_path + "/dockerfiles/{}".format(runtime)))


def main():
    from .utils import main_with_cmds

    cmds = {
        "build": build_runtimes,
        "create": create_dockerfiles,
    }
    main_with_cmds(cmds)


if __name__ == "__main__":
    main()
