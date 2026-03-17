#include "types.hpp"
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#define MAX_CHUNK_SIZE (10 * 1'024 * 1'024)
#define MAX_MERGE_SIZE (MAX_CHUNK_SIZE / 2)

void print_progress(const std::string &operation, const size_t current,
                    const size_t total) {
  int percentage = total > 0 ? (current * 100 / total) : 0;
  int bar_width = 50;
  int filled = bar_width * current / (total > 0 ? total : 1);

  std::cout << "\r" << operation << " [";
  for (int i = 0; i < bar_width; ++i) {
    std::cout << (i < filled ? "=" : (i == filled ? ">" : " "));
  }
  std::cout << "] " << percentage << "% (" << current << "/" << total << ")";
  std::cout << std::flush;

  if (current >= total) {
    std::cout << std::endl;
  }
}

bool save_chunk(const std::string &filename, const std::vector<Point> &data) {
  std::ofstream chunk_file(filename);
  if (!chunk_file.is_open()) {
    std::cout << "Failed to open file!\n";
    return false;
  }

  chunk_file << std::fixed << std::setprecision(13);
  chunk_file << "x, y, z\n";
  for (const auto &p : data) {
    chunk_file << p.x << ',' << p.y << ',' << p.z << '\n';
  }

  chunk_file.close();
  return true;
}

void sort_by_chunks(const std::string &filename, const std::string &chunks_dir,
                    const size_t allowed_chunk_size = MAX_CHUNK_SIZE) {
  std::ifstream data_file(filename);
  if (!data_file.is_open()) {
    std::cout << "Failed to open file!\n";
    return;
  }

  data_file.seekg(0, std::ios::end);
  size_t total_size = data_file.tellg();
  data_file.seekg(0, std::ios::beg);

  std::string header;
  if (!std::getline(data_file, header)) {
    std::cout << "Failed to read header from input file!\n";
    return;
  }

  int chunk_idx = 0;
  size_t current_chunk_size = 0;

  std::string line;
  std::vector<Point> current_chunk_data;

  size_t bytes_processed = header.size() + 1;
  size_t lines_processed = 0;

  const size_t update_interval = 10'000;

  while (std::getline(data_file, line)) {
    size_t line_size = line.size() + 1;

    if (current_chunk_size + line_size > allowed_chunk_size &&
        !current_chunk_data.empty()) {
      std::sort(current_chunk_data.begin(), current_chunk_data.end(),
                [](const Point &p1, const Point &p2) {
                  if (p1.x == p2.x) {
                    return p1.y < p2.y;
                  }
                  return p1.x < p2.x;
                });

      std::ostringstream oss;

      oss << chunks_dir << "/chunk_" << chunk_idx << ".csv";
      if (!save_chunk(oss.str(), current_chunk_data)) {
        std::cout << "Failed to save chunk!\n";
        data_file.close();
        return;
      }

      current_chunk_data.clear();
      current_chunk_size = 0;
      chunk_idx++;
    }

    current_chunk_data.emplace_back(Point::parse(line));
    current_chunk_size += line_size;
    bytes_processed += line_size;
    lines_processed++;

    if (lines_processed % update_interval == 0) {
      print_progress("Sorting", bytes_processed, total_size);
    }
  }

  if (!current_chunk_data.empty()) {
    std::sort(current_chunk_data.begin(), current_chunk_data.end(),
              [](const Point &p1, const Point &p2) {
                if (p1.x == p2.x) {
                  return p1.y < p2.y;
                }
                return p1.x < p2.x;
              });

    std::ostringstream oss;
    oss << chunks_dir << "/chunk_" << chunk_idx << ".csv";
    if (!save_chunk(oss.str(), current_chunk_data)) {
      std::cout << "Failed to save sorted chunk!\n";
      data_file.close();
      return;
    }
  }

  print_progress("Sorting", total_size, total_size);
  data_file.close();
  std::ostringstream oss;
  oss << "Successfully sorted " << filename << " into " << chunk_idx + 1
      << " chunks in directory: " << chunks_dir << '\n';
  std::cout << oss.str();

  return;
}

std::vector<Point> load_data(std::ifstream &file, const size_t allowed_size) {
  std::vector<Point> data;
  size_t current_size = 0;
  std::string line, word;

  while (true) {
    const std::streampos line_start = file.tellg();
    if (!std::getline(file, line)) {
      break;
    }

    const size_t line_size = line.size() + 1;
    if (current_size + line_size > allowed_size && !data.empty()) {
      file.clear();
      file.seekg(line_start);
      break;
    }

    data.emplace_back(Point::parse(line));
    current_size += line_size;
  }

  return data;
}

bool merge_two_chunks(const std::string &chunk1_path,
                      const std::string &chunk2_path,
                      const std::string &output_path,
                      const size_t allowed_merge_size) {
  std::ifstream chunk1(chunk1_path);
  std::ifstream chunk2(chunk2_path);
  std::ofstream output(output_path);

  if (!chunk1.is_open() || !chunk2.is_open() || !output.is_open()) {
    std::cout << "Failed to open files for merging\n";
    return false;
  }

  std::string header;
  std::getline(chunk1, header);
  std::getline(chunk2, header);
  output << "x,y,z\n";
  output << std::fixed << std::setprecision(13);

  std::vector<Point> d1 = load_data(chunk1, allowed_merge_size);
  std::vector<Point> d2 = load_data(chunk2, allowed_merge_size);
  size_t i1 = 0, i2 = 0;

  while (true) {
    if (i1 >= d1.size() && chunk1.good()) {
      d1 = load_data(chunk1, allowed_merge_size);
      i1 = 0;
    }
    if (i2 >= d2.size() && chunk2.good()) {
      d2 = load_data(chunk2, allowed_merge_size);
      i2 = 0;
    }

    if (i1 >= d1.size() && i2 >= d2.size()) {
      break;
    }

    if (i1 < d1.size() && i2 < d2.size()) {
      const Point &p1 = d1[i1];
      const Point &p2 = d2[i2];

      if (p1.x < p2.x || (p1.x == p2.x && p1.y < p2.y)) {
        output << p1.x << ',' << p1.y << ',' << p1.z << '\n';
        i1++;
      } else {
        output << p2.x << ',' << p2.y << ',' << p2.z << '\n';
        i2++;
      }
    } else if (i1 < d1.size()) {
      const Point &p1 = d1[i1];
      output << p1.x << ',' << p1.y << ',' << p1.z << '\n';
      i1++;
    } else if (i2 < d2.size()) {
      const Point &p2 = d2[i2];
      output << p2.x << ',' << p2.y << ',' << p2.z << '\n';
      i2++;
    }
  }
  chunk1.close();
  chunk2.close();
  output.close();
  return true;
}

bool merge_chunks(const std::string &chunks_dir, const std::string &merged_dir,
                  const size_t allowed_merge_size = MAX_MERGE_SIZE) {

  std::filesystem::create_directories(merged_dir);

  std::vector<std::string> chunk_names;
  for (const auto &item : std::filesystem::directory_iterator(chunks_dir)) {
    if (!item.is_regular_file()) {
      continue;
    }
    chunk_names.emplace_back(item.path().string());
  }

  if (chunk_names.empty()) {
    std::cout << "No chunks found to merge.\n";
    return false;
  }

  std::sort(chunk_names.begin(), chunk_names.end());

  auto is_temp_merge_file = [&](const std::string &path) {
    const std::string prefix =
        (std::filesystem::path(merged_dir) / "tmp_merge_").string();
    return path.rfind(prefix, 0) == 0;
  };

  size_t total_merges = chunk_names.size() - 1;
  size_t completed_merges = 0;

  std::vector<std::string> current_level = chunk_names;
  int temp_idx = 0;

  while (current_level.size() > 1) {
    std::vector<std::string> next_level;

    for (size_t i = 0; i < current_level.size(); i += 2) {
      if (i + 1 >= current_level.size()) {
        next_level.push_back(current_level[i]);
        continue;
      }

      std::ostringstream oss;
      oss << merged_dir << "/tmp_merge_" << temp_idx++ << ".csv";
      const std::string merged_chunk_name = oss.str();

      if (merge_two_chunks(current_level[i], current_level[i + 1],
                           merged_chunk_name, allowed_merge_size)) {
        next_level.push_back(merged_chunk_name);
        completed_merges++;
        print_progress("Mergind chunks", completed_merges, total_merges);

        // Delete intermediate merge inputs after successful merge.
        if (is_temp_merge_file(current_level[i])) {
          std::filesystem::remove(current_level[i]);
        }
        if (is_temp_merge_file(current_level[i + 1])) {
          std::filesystem::remove(current_level[i + 1]);
        }
      }
    }
    current_level = std::move(next_level);
  }

  if (current_level.empty()) {
    std::cout << "Merge failed: no output produced.\n";
    return false;
  }

  const std::string final_merged_path =
      (std::filesystem::path(merged_dir) / "merged_final.csv").string();

  if (std::filesystem::exists(final_merged_path)) {
    std::filesystem::remove(final_merged_path);
  }

  // If final file is a temp merge output, rename it. Otherwise copy source
  // chunk.
  if (is_temp_merge_file(current_level[0])) {
    std::filesystem::rename(current_level[0], final_merged_path);
  } else {
    std::filesystem::copy_file(
        current_level[0], final_merged_path,
        std::filesystem::copy_options::overwrite_existing);
  }

  std::cout << "Merge complete: " << final_merged_path << "\n";
  return true;
}

bool skip_meta(std::ifstream &f) {
  std::string line;
  while (std::getline(f, line)) {
    if (line == "# META END") {
      std::getline(f, line);
      return true;
    }
  }
  return false;
}

static void write_meta_block(std::ofstream &out, const NodeMeta &meta) {
  out << std::fixed << std::setprecision(13);
  out << "# META BEGIN\n";
  out << "# path        " << meta.node_path << "\n";
  out << "# depth       " << meta.depth << "\n";
  out << "# is_leaf     " << (meta.is_leaf ? 1 : 0) << "\n";
  out << "# split_axis  "
      << (meta.axis == Axis::X   ? "x"
          : meta.axis == Axis::Y ? "y"
                                 : "none")
      << "\n";

  out << "# split_value ";
  if (meta.split_value) {
    out << *meta.split_value;
  } else {
    out << "none";
  }
  out << '\n';

  out << "# xmin        " << meta.bounds.xmin << "\n";
  out << "# xmax        " << meta.bounds.xmax << "\n";
  out << "# ymin        " << meta.bounds.ymin << "\n";
  out << "# ymax        " << meta.bounds.ymax << "\n";
  out << "# child_left  "
      << (meta.children.left.empty() ? "none" : meta.children.left) << "\n";
  out << "# child_right "
      << (meta.children.right.empty() ? "none" : meta.children.right) << "\n";
  out << "# META END\n";

  if (meta.is_leaf) {
    out << "x,y,z\n";
  }
}

static bool prepend_meta_to_leaf(const std::string &filepath,
                                 const NodeMeta &meta) {
  std::ifstream in(filepath);
  if (!in.is_open()) {
    std::cout << "Failed to open in file!\n";
    return false;
  }

  std::string first;
  std::getline(in, first);
  if (first == "# META BEGIN") {
    skip_meta(in);
  }

  std::string temp = filepath + ".tmp";
  std::ofstream out(temp);
  if (!out.is_open()) {
    std::cout << "Failed to open out file!\n";
    return false;
  }

  write_meta_block(out, meta);

  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty()) {
      out << line << '\n';
    }
  }

  in.close();
  out.close();
  std::filesystem::rename(temp, filepath);
  return true;
}

static bool write_internal_node_leaf_file(const std::string &filepath,
                                          const NodeMeta &meta) {
  std::ofstream f(filepath);
  if (!f.is_open()) {
    std::cout << "Failed to open file!\n";
    return false;
  }
  write_meta_block(f, meta);
  return true;
}

static std::optional<Bounds> scan_bounds(const std::string &filepath) {
  std::ifstream f(filepath);
  if (!f.is_open()) {
    std::cout << "Failed to open file!\n";
    return std::nullopt;
  }

  std::string first;
  std::getline(f, first);
  if (first == "# META BEGIN") {
    skip_meta(f);
  }

  bool any = false;
  Bounds b{};
  std::string line;
  while (std::getline(f, line)) {
    if (line.empty()) {
      continue;
    }

    const Point p = Point::parse(line);
    if (!any) {
      b.xmin = b.xmax = p.x;
      b.ymin = b.ymax = p.y;
      any = true;
    } else {
      b.xmin = std::min(b.xmin, p.x);
      b.xmax = std::max(b.xmax, p.x);
      b.ymin = std::min(b.ymin, p.y);
      b.ymax = std::max(b.ymax, p.y);
    }
  }

  if (!any) {
    return std::nullopt;
  }
  return b;
}

bool build_tree_from_chunks(const std::vector<std::string> &leaf_files,
                            const std::string &tree_dir) {
  if (leaf_files.empty()) {
    std::cout << "No leaf files provided.\n";
    return false;
  }

  std::filesystem::create_directories(tree_dir);
  const size_t n = leaf_files.size();

  std::cout << "Phase 1: scanning " << n << " leaf files...\n";

  std::vector<std::unique_ptr<Node>> level;
  level.reserve(n);

  for (size_t i = 0; i < n; ++i) {
    const auto bounds_opt = scan_bounds(leaf_files[i]);
    if (!bounds_opt) {
      std::cout << "Warning: skiping empty/unreadable file" << leaf_files[i]
                << "\n";
      continue;
    }

    std::ostringstream oss;
    oss << tree_dir << "/leaf_" << i << ".csv";
    std::string leaf_path = oss.str();

    if (std::filesystem::absolute(leaf_files[i]) !=
        std::filesystem::absolute(leaf_path)) {
      std::filesystem::rename(leaf_files[i], leaf_path);
    }

    std::string leaf_name = "leaf_" + std::to_string(i);

    NodeMeta leaf_meta;
    leaf_meta.node_path = leaf_name;
    leaf_meta.depth = 0;
    leaf_meta.is_leaf = true;
    leaf_meta.axis = Axis::NONE;
    leaf_meta.split_value = std::nullopt;
    leaf_meta.bounds = *bounds_opt;
    leaf_meta.children = {"", ""};

    prepend_meta_to_leaf(leaf_path, leaf_meta);

    auto node = std::make_unique<Node>();
    node->path = leaf_name;
    node->filename = leaf_path;
    node->is_leaf = true;
    node->xmin = bounds_opt->xmin;
    node->xmax = bounds_opt->xmax;
    node->ymin = bounds_opt->ymin;
    node->ymax = bounds_opt->ymax;

    level.push_back(std::move(node));
    print_progress("Scanning", i + 1, n);
  }

  if (level.empty()) {
    return false;
  }

  std::cout << "Phase 2: assembling tree (" << level.size() << " leaves)...\n";

  int bottom_up_level = 0;
  int internal_idx = 0;

  while (level.size() > 1) {
    ++bottom_up_level;
    Axis axis = (bottom_up_level % 2 == 1) ? Axis::X : Axis::Y;

    std::vector<std::unique_ptr<Node>> next_level;
    next_level.reserve((level.size() + 1) / 2);

    for (size_t i = 0; i < level.size(); i += 2) {
      if (i + 1 >= level.size()) {
        next_level.push_back(std::move(level[i]));
        continue;
      }

      Node *L = level[i].get();
      Node *R = level[i + 1].get();

      const double xmin = std::min(L->xmin, R->xmin);
      const double xmax = std::max(L->xmax, R->xmax);
      const double ymin = std::min(L->ymin, R->ymin);
      const double ymax = std::max(L->ymax, R->ymax);

      const double split_val = (axis == Axis::X) ? R->xmin : (ymin + ymax) / 2;

      std::ostringstream oss;
      oss << tree_dir << "/node_" << bottom_up_level << "_" << internal_idx
          << ".csv";
      const std::string node_file = oss.str();
      const std::string node_path_str = "node_" +
                                        std::to_string(bottom_up_level) + "_" +
                                        std::to_string(internal_idx);

      const std::string left_base =
          std::filesystem::path(L->filename).filename().string();
      const std::string right_base =
          std::filesystem::path(R->filename).filename().string();

      NodeMeta internal_meta;
      internal_meta.node_path = node_path_str;
      internal_meta.depth = 0;
      internal_meta.is_leaf = false;
      internal_meta.axis = axis;
      internal_meta.split_value = split_val;
      internal_meta.bounds = Bounds{xmin, xmax, ymin, ymax};
      internal_meta.children = {left_base, right_base};

      write_internal_node_leaf_file(node_file, internal_meta);

      auto parent = std::make_unique<Node>();
      parent->path = node_path_str;
      parent->filename = node_file;
      parent->is_leaf = false;
      parent->axis = axis;
      parent->split_value = split_val;
      parent->xmin = xmin;
      parent->xmax = xmax;
      parent->ymin = ymin;
      parent->ymax = ymax;
      parent->left = std::move(level[i]);
      parent->right = std::move(level[i + 1]);

      next_level.push_back(std::move(parent));
      ++internal_idx;
    }

    level = std::move(next_level);
  }

  std::unique_ptr<Node> root = std::move(level[0]);

  std::cout << "Phase 3: stamping depths...\n";

  std::function<void(Node *, int)> fix_depth = [&](Node *node, int depth) {
    if (!node)
      return;

    node->depth = depth;

    const std::string left_base =
        node->left
            ? std::filesystem::path(node->left->filename).filename().string()
            : "";
    const std::string right_base =
        node->right
            ? std::filesystem::path(node->right->filename).filename().string()
            : "";

    if (node->is_leaf) {
      NodeMeta leaf_meta;
      leaf_meta.node_path = node->path;
      leaf_meta.depth = depth;
      leaf_meta.is_leaf = true;
      leaf_meta.axis = Axis::NONE;
      leaf_meta.split_value = std::nullopt;
      leaf_meta.bounds = Bounds{node->xmin, node->xmax, node->ymin, node->ymax};
      leaf_meta.children = {"", ""};

      prepend_meta_to_leaf(node->filename, leaf_meta);
    } else {
      NodeMeta internal_meta;
      internal_meta.node_path = node->path;
      internal_meta.depth = depth;
      internal_meta.is_leaf = false;
      internal_meta.axis = node->axis;
      internal_meta.split_value = node->split_value;
      internal_meta.bounds =
          Bounds{node->xmin, node->xmax, node->ymin, node->ymax};
      internal_meta.children = {left_base, right_base};

      write_internal_node_leaf_file(node->filename, internal_meta);
    }

    fix_depth(node->left.get(), depth + 1);
    fix_depth(node->right.get(), depth + 1);
  };

  fix_depth(root.get(), 0);

  const std::string root_dest = tree_dir + "/node_root.csv";

  std::filesystem::rename(root->filename, root_dest);
  root->filename = root_dest;

  NodeMeta root_meta;
  root_meta.node_path = "node_root";
  root_meta.depth = root->depth;
  root_meta.is_leaf = false;
  root_meta.axis = root->axis;
  root_meta.split_value = root->split_value;
  root_meta.bounds = Bounds{root->xmin, root->xmax, root->ymin, root->ymax};
  root_meta.children = {
      root->left
          ? std::filesystem::path(root->left->filename).filename().string()
          : "",
      root->right
          ? std::filesystem::path(root->right->filename).filename().string()
          : ""};

  write_internal_node_leaf_file(root->filename, root_meta);

  std::cout << "Tree built successfully. Root: " << root_dest << '\n';
  return true;
}

std::unique_ptr<Node> load_tree_index(const std::string &tree_dir) {
  std::function<std::unique_ptr<Node>(const std::string &)> load_node =
      [&](const std::string &filepath) -> std::unique_ptr<Node> {
    std::ifstream f(filepath);
    if (!f.is_open()) {
      return nullptr;
    }

    auto node = std::make_unique<Node>();
    node->filename = filepath;
    std::string child_left_name, child_right_name, line;
    while (std::getline(f, line)) {
      if (line == "# META BEGIN") {
        continue;
      }
      if (line == "# META END") {
        break;
      }
      if (line.rfind("# ", 0) != 0) {
        continue;
      }

      std::istringstream iss(line.substr(2));
      std::string key;
      iss >> key;

      if (key == "path") {
        iss >> node->path;
      } else if (key == "depth") {
        iss >> node->depth;
      } else if (key == "is_leaf") {
        int v;
        iss >> v;
        node->is_leaf = v;
      } else if (key == "split_axis") {
        std::string ax;
        iss >> ax;
        node->axis = (ax == "x") ? Axis::X : (ax == "y") ? Axis::Y : Axis::NONE;
      } else if (key == "split_value") {
        std::string sv;
        iss >> sv;
        if (sv != "none")
          node->split_value = std::stod(sv);
      } else if (key == "xmin") {
        iss >> node->xmin;
      } else if (key == "xmax") {
        iss >> node->xmax;
      } else if (key == "ymin") {
        iss >> node->ymin;
      } else if (key == "ymax") {
        iss >> node->ymax;
      } else if (key == "child_left") {
        iss >> child_left_name;
        if (child_left_name == "none")
          child_left_name = "";
      } else if (key == "child_right") {
        iss >> child_right_name;
        if (child_right_name == "none")
          child_right_name = "";
      }
    }

    if (!child_left_name.empty()) {
      node->left = load_node(tree_dir + "/" + child_left_name);
    }

    if (!child_right_name.empty()) {
      node->right = load_node(tree_dir + "/" + child_right_name);
    }

    return node;
  };

  return load_node(tree_dir + "/node_root.csv");
}

std::vector<std::string> query_tree(const Node *root, Bounds bounds) {
  std::vector<std::string> result;
  if (!root) {
    std::cout << "Root not found\n";
    return result;
  }

  std::function<void(const Node *)> traverse = [&](const Node *node) {
    if (!node) {
      return;
    }

    if (node->xmax < bounds.xmin || node->xmin > bounds.xmax ||
        node->ymax < bounds.ymin || node->ymin > bounds.ymax) {
      return;
    }

    if (node->is_leaf || (!node->left && !node->right)) {
      result.push_back(node->filename);
      return;
    }

    traverse(node->left.get());
    traverse(node->right.get());
  };

  traverse(root);
  return result;
}

int main(int argc, char **argv) {
  if (argc < 3) {
    std::cout << "Usage: ./DAM [-s <input_file>] [-t <input_file>] [-q "
                 "<tree_dir> <xmin> <xmax> <ymin> <ymax>]\n"
              << "\t-s: Sort the input file\n"
              << "\t-t: Build spatial tree from input file\n"
              << "\t-q: Query data from tree root";
    return 1;
  }

  const std::string option = argv[1];
  const std::string input_file = argv[2];

  if (option == "-q" && argc != 7) {
    std::cout << "Error: -q missing arguments";
    return 1;
  }

  if (option == "-s") {
    std::filesystem::create_directories("data/sorted");
    sort_by_chunks(input_file, "data/sorted");

    std::filesystem::create_directories("data/merged");
    if (merge_chunks("data/sorted", "data/merged")) {
      std::filesystem::remove_all("data/sorted");
    }

  } else if (option == "-t") {
    std::filesystem::create_directories("data/sorted");
    sort_by_chunks(input_file, "data/sorted");

    std::vector<std::string> leaf_files;
    for (const auto &entry :
         std::filesystem::directory_iterator("data/sorted")) {
      if (entry.is_regular_file()) {
        leaf_files.emplace_back(entry.path().string());
      }
    }

    std::sort(leaf_files.begin(), leaf_files.end());

    if (!build_tree_from_chunks(leaf_files, "data/tree")) {
      std::cout << "Tree build failed.\n";
      return 1;
    }

    std::filesystem::remove_all("data/sorted");

  } else if (option == "-q") {
    auto root = load_tree_index(input_file);

    Bounds bounds;
    bounds.xmin = std::stod(argv[3]);
    bounds.xmax = std::stod(argv[4]);
    bounds.ymin = std::stod(argv[5]);
    bounds.ymax = std::stod(argv[6]);

    std::cout << std::fixed << std::setprecision(13);

    std::cout << "Bounds: " << bounds.xmin << ", " << bounds.xmax << ", "
              << bounds.ymin << ", " << bounds.ymax << "\n";

    auto candidates = query_tree(root.get(), bounds);
    if (candidates.empty()) {
      std::cout << "No candidates found.\n";
      return 0;
    }

    for (const auto &fpath : candidates) {
      std::ifstream f(fpath);
      skip_meta(f);
      std::string line;
      while (std::getline(f, line)) {
        if (line.empty()) {
          continue;
        }
        Point p = Point::parse(line);
        if (p.x >= bounds.xmin && p.x <= bounds.xmax && p.y >= bounds.ymin &&
            p.y <= bounds.ymax) {
          std::cout << p.x << ", " << p.y << ", " << p.z << '\n';
        }
      }
    }
  } else {
    std::cout << "Usage: ./DAM [-s <input_file>] [-t <input_file>] [-q "
                 "<tree_root> <xmin> <xmax> <ymin> <ymax>]\n"
              << "\t-s: Sort the input file\n"
              << "\t-t: Build spatial tree from input file\n"
              << "\t-q: Query data from tree root";
    return 1;
  }

  return 0;
}