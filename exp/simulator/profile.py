import cProfile
import pstats
import os
from .experiments import _exp_lat_cdf
_dir_path = os.path.dirname(os.path.realpath(__file__))
_stats_file = _dir_path + "/exp_stats"

def main():
    import pstats
    cProfile.run("_exp_lat_cdf()", _stats_file)
    p = pstats.Stats(_stats_file)
    p.sort_stats('cumulative').print_stats(25)

if __name__ == "__main__":
    main()