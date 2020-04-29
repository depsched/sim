from build.kube.utils import cmd
import os

_dir_path = os.path.dirname(os.path.realpath(__file__))
_script_dir = _dir_path + "/scripts/"
os.chdir(_script_dir)


def ecr_login():
    cmd_with_bash(_script_dir + "login-ecr.sh")


def cmd_with_bash(command):
    cmd("bash {}".format(command))


def main():
    from build.kube.utils import main_with_cmds
    cmds = {
        "ecr": ecr_login,
    }

    main_with_cmds(cmds)


if __name__ == "__main__":
    main()
