import analyze
import pprint as pp

def make_layer_share_plot():
    """produce plot-ready .csv for layer sharing analysis"""
    out_file = "./result/layer_share.csv"
    az, lines = analyze.Analyzer(), []
    total_count = az.query_layer_count()
    for share_count in range(0, 51, 10):
        if share_count == 0:
            share_count += 1
        query_result = az.query_layer_top_size(share_count)
        # the layer ratio reports percentage
        layer_ratio = str(query_result[2])
        # total size reports in mb
        total_size = str(query_result[3]/1000)
        # compose the line
        line = [str(share_count), layer_ratio, total_size]
        # append the meta-data
        if not lines:
            line.append("count|percentage|size in gbytes")
        lines.append(",".join(line) + "\n")

    with open(out_file, "w") as f:
        f.writelines(lines)

# def make_image_version_plot():


def main():
    make_layer_share_plot()

if __name__ == "__main__":
    main()