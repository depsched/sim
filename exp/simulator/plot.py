#!/usr/bin/env python3

from __future__ import print_function

import glob
import os

from .__plot__.plot_gen import plot_hist_param_sweep
from .utils import cmd

"""
- generate (gnu)plot-ready .csv files; the script should be exec in its directory

- directory structure as the following:
    result/run_cont_cap:
    uniform  pop

    result/run_cont_cap/uniform|pop:
    16  2  4  8

    result/run_lat_cdf/uniform|pop:     
    out_lat_dep_20171223142317.csv     out_lat_percentiles_dep_20171223142317.csv     out_meta_dep_20171223142317.csv ...
"""

_dir_path = os.path.dirname(os.path.realpath(__file__))
_raw_result_path = _dir_path + "/__result__/"
_result_path = _dir_path + "/__plot__/__data__/"
_template_path = _dir_path + "/__plot__/__template__/"
_plot_path = _dir_path + "/__plot__/__pdf__/"

exps = ["store_size", "cluster_size", "req_rate"]
# exps = ["cluster_size"]
# sample_mode = ["uniform", "zipf", "pop"]
sample_mode = ["zipf", "zipf", "zipf"]
ordered_policies = ["dep", "kube", "monkey"]


class LatPlotter:
    def __init__(self):
        pass

    def _parse_hist(self, data_files):
        """ parse csv files, and return data entries as
        a list of lines; the order of a line is in form
        <x,yboxlow,ylow,yhigh,yboxhigh,mean,median> or
        <x,25,5,95,75,50,mean>, <...> (uniform|pop) [or
        <x,50,25,95,75,mean,mean>, <...> (uniform|pop)]
        * policies; the data_files contain file handlers
        sorted by policy, then runs small to large, and
        finally the sample mode """

        rows, row, counter = [], [0], 0
        for data_file in data_files:
            data_list = data_file.readline().rstrip().split(",")
            for i in range(len(data_list)):
                data_list[i] = self.format_sec(float(data_list[i]))
            data_file.close()
            row += [data_list[2], data_list[1],
                    data_list[4], data_list[3],
                    data_list[5], data_list[5]]
            counter += 1
            if counter % 3 == 0:
                rows.append(row)
                row = [counter / 3]
        return rows

    def _parse_cdf(self, data_files):
        """ parse csv files, sort, append cdf indices, and
        return plot ready list of rows in tuple """

        columns, num_rows = [], 0
        for data_file in data_files:
            data_list = sorted([int(d.rstrip())
                                for d in data_file.readlines()])
            data_file.close()
            columns.append(data_list)
            num_rows = len(data_list)
        cdf_indices = [(i + 1) / float(num_rows) for i in range(num_rows)]
        columns.insert(0, cdf_indices)
        return zip(*columns)

    def _parse_meta(self, data_files, metrics=set({"rej_ratio"})):
        """ assume one line one data; data files are ordered by policies """
        rows, row, counter = [], [0], 0
        for data_file in data_files:
            data_lines = data_file.readlines()
            for data_line in data_lines:
                data_line = data_line.rstrip("\n").split(",")
                data_type, data = data_line[0], data_line[1]
                if data_type not in metrics:
                    continue
                row.append(data)
            counter += 1
            if counter % 2 == 0:
                rows.append(row)
                row = [counter / 2]
        return rows

    def _parse_ratio(self, data_files):
        row, rows, counter = [], [], 0
        for data_file in data_files:
            counter += 1
            data_list = data_file.readline().rstrip("\n").split(",")
            mean = float(data_list[5])
            row += [mean]
            if counter % 3 == 0 and counter != 0:
                rows.append(row)
                row = []
                counter = 0
        ratio_rows = []
        for row in rows:
            agn_mean = row[2]
            ratio_rows.append([agn_mean/row[0],
                               agn_mean/row[1]])
        # print(ratio_rows)
        return ratio_rows

    def gen_hist_sweep(self):
        """ parameter sweep histograms and meta data """
        out_file = _result_path + "param_sweep_{}_{}.csv"
        exp_path = _raw_result_path + "run_{}/{}/"
        path_suffix = "/out_lat_percentiles_{}*.csv"

        out_file_meta = _result_path + "param_sweep_{}_{}_meta.csv"
        path_suffix_meta = "/out_meta_{}*.csv"

        for exp in exps:
            data_files = []
            for policy in ordered_policies:
                mode_files = []
                mode_files_meta = []
                for mode in sample_mode:
                    run_paths = glob.glob(exp_path.format(exp, mode) + "*")
                    reverse = False
                    if exp in ["cont_cap"]:
                        reverse = True
                    run_paths = sorted(run_paths, key=lambda x: float(
                        x.split("/")[-1]), reverse=reverse)
                    files = []
                    files_meta = []
                    for run_path in run_paths:
                        file_name = sorted(glob.glob(
                            run_path + path_suffix.format(policy)), key=os.path.getmtime, reverse=True)[0]
                        file_name_meta = sorted(glob.glob(
                            run_path + path_suffix.format(policy)), key=os.path.getmtime, reverse=True)[0]
                        files.append(open(file_name, "r"))
                        files_meta.append(open(file_name_meta, "r"))
                    mode_files.append(files)
                    mode_files_meta.append(files_meta)
                # import pprint as pp
                # pp.pprint(mode_files)
                data_files = [j for i in zip(*mode_files) for j in i]
                # pp.pprint(data_files)
                # exit()
                # print("before", len(data_files))
                lines = self._parse_hist(data_files)
                self.dump_csv(out_file.format(exp, policy), lines)

                data_files = [j for i in zip(*mode_files_meta) for j in i]
                lines = self._parse_meta(data_files)
                self.dump_csv(out_file_meta.format(exp, policy), lines)
        cmd("chmod 666 {}*".format(_result_path))
        cmd("chmod -R 666 {}*".format(_result_path))

        self.gen_hist_sweep_ratio()
        for exp in exps:
            for mode in sample_mode:
                plot_hist_param_sweep(exp, mode, ratio=True)
                plot_hist_param_sweep(exp, mode, ratio=False)
        cmd("cd {}; mv *.pdf ".format(_result_path) + _plot_path)
        cmd("cd {}; chmod 777 *.pdf ".format(_plot_path))

    def gen_cdf(self):
        out_file = _result_path + "lat_cdf_{}.csv"
        path = _raw_result_path + "run_lat_cdf/{}/out_lat_{}*.csv"

        for mode in sample_mode:
            data_files = []
            for policy in ordered_policies:
                print(path.format(mode, policy))
                try:
                    file_name = glob.glob(path.format(mode, policy))[0]
                    data_files.append(open(file_name, "r"))
                except:
                    print("missing {}".format(path.format(mode, policy)))
            lines = self._parse_cdf(data_files)
            self.dump_csv(out_file.format(mode), lines)

        def f(mode):
            cmd("yes | cp {}{} {}{}".format(_template_path, "cdf_template_{}.plt".format(mode),
                                            _result_path, "lat_cdf_{}.plt".format(mode)))
            cmd("cd {}; gnuplot {}{}".format(_result_path, _result_path, "lat_cdf_{}.plt".format(mode)))
            cmd("mv {}{} {}".format(_result_path, "lat_cdf_{}.pdf".format(mode), _plot_path))

        f("uniform")
        f("zipf")
        f("pop")
        cmd("cd {}; chmod 777 *.pdf ".format(_plot_path))

    def gen_hist_sweep_ratio(self):
        """ parameter sweep ratio, with baseline dep """
        out_file = _result_path + "param_sweep_{}_{}_ratio.csv"
        exp_path = _raw_result_path + "run_{}/{}/"
        path_suffix = "/out_lat_percentiles_{}*.csv"

        for exp in exps:
            for mode in sample_mode:
                data_files = []
                run_paths = glob.glob(exp_path.format(exp, mode) + "*")
                reverse = False
                if exp in ["cont_cap"]:
                    reverse = True
                run_paths = sorted(run_paths, key=lambda x: float(
                    x.split("/")[-1]), reverse=reverse)
                for run_path in run_paths:
                    for policy in ordered_policies:
                        file_name = glob.glob(
                            run_path + path_suffix.format(policy))[0]
                        data_files.append(open(file_name, "r"))

                # print(exp, mode)
                lines = self._parse_ratio(data_files)
                self.dump_csv(out_file.format(exp, mode), lines)

    def gen_all(self):
        self.gen_cdf()
        self.gen_hist_sweep()
        self.gen_hist_sweep_ratio()

    def dump_csv(self, file_name, lines):
        with open(file_name, "w") as f:
            # print("after", file_name, len(lines))
            for line in lines:
                f.write(",".join([str(i) for i in line]) + "\n")

    def format_sec(self, value):
        # return value in second
        return round(value / 1000, 1)


def plot_hist_param_sweep(sweep_type, sample_mode, re_gen=True, ratio=False):
    def _init(plot_base_file, plot_file):
        cmd("cp {} {}".format(plot_base_file, plot_file))

    def _plot(out_file):
        plot_cmd = "cd {}; gnuplot {}".format(_result_path, out_file)
        cmd(plot_cmd)
        cmd("echo " + _plot_path)
        cmd("cd " + _plot_path + "; chmod 777 *.pdf")

    if ratio:
        in_files = [_result_path + "param_sweep_{}_{}_ratio.csv".format(sweep_type, sample_mode)]
        template_file, out_file = _template_path + "param_sweep_ratio_base.plt", \
                                  _plot_path + "hist_param_sweep_{}_{}_ratio.pdf".format(sweep_type, sample_mode)
        plot_file = _result_path + "hist_param_sweep_ratio_{}_{}.plt".format(sweep_type, sample_mode)
    else:
        in_files = [_result_path + "param_sweep_{}_{}.csv".format(sweep_type, p) for p in ["dep", "kube", "monkey"]]
        in_files_meta = [_result_path + "param_sweep_{}_{}_meta.csv".format(sweep_type, p) for p in
                         ["dep", "kube", "monkey"]]
        template_file, out_file = _template_path + "hist_param_sweep_{}_base.plt".format(
            sample_mode), _result_path + "hist_param_sweep_{}_{}.pdf".format(sweep_type, sample_mode)
        plot_file = _result_path + "hist_param_sweep_{}_{}.plt".format(sweep_type, sample_mode)

    # if not re_gen:
    #     _plot(plot_file)
    #     return
    skip_set = {"hist_param_sweep_ratio_store_size_pop.plt","hist_param_sweep_ratio_cluster_size_zipf.plt"}
    if plot_file.split("/")[-1] in skip_set:
        print("direct plot {}".format(plot_file))
        _plot(plot_file)
        return

    _init(template_file, plot_file)

    # read the template file, generate a new plot file
    with open(template_file, "r") as f, open(plot_file, "w") as g:
        is_param = True
        for line in f.readlines():
            line = line.rstrip()
            if "#@" in line:
                is_param = False
            if "#@" in line:
                if ratio:
                    line += "\nset logscale y 2"
                if sweep_type == "cluster_size":
                    line += "\nset xtics ('25' 0, '50' 1, '100' 2, '200' 3, '400' 4, '800' 5, '1000' 6)"
                elif sweep_type == "store_size":
                    line += "\nset xtics ('16' 0, '24' 1, '32' 2, '48' 3, '64' 4)"
                elif sweep_type == "pool":
                    line += "\nset xtics ('(4,64GB)' 0, '(8,32GB)' 1, '(16,16GB)' 2, '(32,8GB)' 3, '(64,4GB)' 4)"
                # done line
                elif sweep_type == "cont_length":
                    line += "\nset xtics ('1' 0, '2' 1, '5' 2, '25' 3, '50' 4)"
                elif sweep_type == "req_rate":
                    # line += "\nset xtics ('10' 0, '20' 1, '40' 2, '60' 3, '80' 4)"
                    if sample_mode == "zipf":
                        line += "\nset xtics ('40' 0, '60' 1, '80' 2, '100' 3, '125' 4, '150' 5)"
                    elif sample_mode == "uniform":
                        line += "\nset xtics ('30' 0, '50' 1, '70' 2, '90' 3, '110' 4, '130' 5)"
                    else:
                        line += "\nset xtics ('60' 0, '100' 1, '140' 2, '180' 3, '215' 4, '240' 5)"
                elif sweep_type == "evict":
                    line += "\nset xtics ('10' 0, '20' 1, '40' 2, '80' 3, '100' 4)"
                elif sweep_type == "cluster_size":
                    line += "\nset xtics ('20' 0, '50' 1, '100' 2, '200' 3, '500' 4, '1000' 5)"
                elif sweep_type == "cluster_size_zipf":
                    line += "\nset xtics ('20' 0, '50' 1, '100' 2, '200' 3, '500' 4, '1000' 5)"
                else:
                    line += "\nset xtics ('32' 0, '16' 1, '8' 2, '4' 3, '2' 4)"
            if not is_param:
                g.writelines(line + "\n")
                continue
            line = line + " "
            if "__INPUT_1__" in line:
                line = line + '"' + in_files[0] + '"'
            elif "__INPUT_2__" in line:
                line = line + '"' + in_files[1] + '"'
            elif "__INPUT_3__" in line:
                line = line + '"' + in_files[2] + '"'
            elif "__INPUT_META_1__" in line:
                line = line + '"' + in_files_meta[0] + '"'
            elif "__INPUT_META_2__" in line:
                line = line + '"' + in_files_meta[1] + '"'
            elif "__INPUT_META_3__" in line:
                line = line + '"' + in_files_meta[2] + '"'
            elif "__OUTPUT__" in line:
                line = line + '"' + out_file + '"'
            elif "__XLABEL__" in line:
                if sweep_type == "cluster_size":
                    line = line + '"' + "Number of Nodes" + '"'
                elif sweep_type in ["store_size"]:
                    line = line + '"' + "Image Store Size (GB)" + '"'
                elif sweep_type in ["pool"]:
                    line = line + '"' + "Configurations" + '"'
                # done line
                elif sweep_type in ["evict"]:
                    line = line + '"' + "Eviction Ratio (%)" + '"'
                elif sweep_type in ["cont_length"]:
                    line = line + '"' + "Task Duration (%)" + '"'
                elif sweep_type in ["cont_cap"]:
                    line = line + '"' + "Container Cap" + '"'
                elif sweep_type in ["req_rate"]:
                    line = line + '"' + "Load (req/s)" + '"'
                else:
                    line = line + '"' + " ".join(sweep_type.split("_")) + '"'
            elif "__YLABEL__" in line:
                if ratio:
                    line = line + '"Speedup (-x)"'
                else:
                    line = line + '"Startup Latency (s)"'
            elif "__TITLE_1__" in line:
                if ratio:
                    line = line + '"Image"'
                else:
                    line = line + '"Layer"'
            elif "__TITLE_2__" in line:
                if ratio:
                    line = line + '"Layer"'
                else:
                    line = line + '"Image"'
            elif "__TITLE_3__" in line:
                line = line + '"Agnostic"'
            elif "__XRANGE_START__" in line:
                line = line + '-0.5'
            elif "__XRANGE_END__" in line:
                if sweep_type == "cluster_size":
                    line += '6.5'
                elif sweep_type == "req_rate":
                    line += '5.5'
                else:
                    line = line + '4.5'

            elif "__YRANGE_START__" in line:
                line = line + '0'
            elif "__YRANGE_END__" in line:
                line = line + '""'
            else:
                line = line + '"NONE"'
            g.writelines(line + "\n")

    _plot(plot_file)


def evict():
    sample_modes = ["uniform", "zipf", "pop"]
    file_name_str = "exp_evict_dep_{}_mean_startup_lat.csv"
    out_file = "exp_evict_dep_summary.csv"
    row = []
    for mode in sample_modes:
        file_name = file_name_str.format(mode)
        with open(file_name, "r") as f:
            lines = f.readlines()
            image_result = lines[0].rstrip().split(",")[1]
            layer_result = lines[1].rstrip().split(",")[1]
            ratio = (int(image_result) - int(layer_result)) / int(layer_result)
            row.append(round(ratio * 100, 1))
    with open(out_file, "w") as f:
        f.writelines(",".join(map(str, row)))
        # print(row)
    cmd("yes | cp {}{} {}{}".format(_template_path, "evict_policy_template.plt",
                                    _result_path, "evict_policy.plt"))
    cmd("cd {}; gnuplot {}{}".format(_result_path, _result_path, "evict_policy.plt"))
    cmd("cd {}; mv *.pdf ".format(_result_path) + _plot_path)
    cmd("cd {}; chmod 777 *.pdf ".format(_plot_path))


def util():
    """Plot both the lat cdf and the store size."""
    sample_modes = ["uniform", "zipf", "pop"]
    file_name_str = "exp_evict_dep_{}_mean_startup_lat.csv"
    out_file = "exp_evict_dep_summary.csv"
    row = []
    for mode in sample_modes:
        file_name = file_name_str.format(mode)
        with open(file_name, "r") as f:
            lines = f.readlines()
            image_result = lines[0].rstrip().split(",")[1]
            layer_result = lines[1].rstrip().split(",")[1]
            ratio = (int(image_result) - int(layer_result)) / int(layer_result)
            row.append(round(ratio * 100, 1))
    with open(out_file, "w") as f:
        f.writelines(",".join(map(str, row)))
        # print(row)

    sample_modes = ["uniform", "zipf", "pop"]
    for mode in sample_modes:

        cmd("yes | cp {}{} {}{}".format(_template_path, "util_store_size_template_{}.plt".format(mode),
                                        _result_path, "util_store_size_{}.plt".format(mode)))
        cmd("cd {}; gnuplot {}{}".format(_result_path, _result_path, "util_store_size_{}.plt".format(mode)))
        cmd("cd {}; mv *.pdf ".format(_result_path) + _plot_path)
        cmd("cd {}; chmod 777 *.pdf ".format(_plot_path))


def load():
    pass


def main():
    from .utils import main_with_cmds
    lp = LatPlotter()
    cmds = {"all": lp.gen_all,
            "cdf": lp.gen_cdf,
            "hist": lp.gen_hist_sweep,
            "evict": evict,
            "hist_ratio": lp.gen_hist_sweep_ratio,
            }

    main_with_cmds(cmds)


if __name__ == "__main__":
    main()
