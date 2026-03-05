#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <print>
#include <sstream>
#include <string>
#include <vector>

constexpr size_t MAX_CHUNK_SIZE = 10 * 1024 * 1024; // 10MB in bytes

struct Point {
  double x, y, z;
};

bool split_into_chunks(const std::string &input_file,
                       const std::string &output_dir) {

  std::ifstream input(input_file);
  if (!input.is_open()) {
    std::println("Failed to open input file: {}", input_file);
    return false;
  }

  std::string header;
  if (!std::getline(input, header)) {
    std::println("Failed to read header from input file");
    return false;
  }

  std::ofstream output_chunk;
  int chunk_number = 1;
  size_t current_chunk_size = 0;
  std::string line;

  auto open_new_chunk = [&]() {
    if (output_chunk.is_open()) {
      output_chunk.close();
    }
    std::string output_file =
        output_dir + "/chunk_" + std::to_string(chunk_number) + ".csv";
    output_chunk.open(output_file);
    if (!output_chunk.is_open()) {
      std::println("Failed to open output file: {}", output_file);
      return false;
    }
    output_chunk << header << '\n';
    current_chunk_size = header.size() + 1;
    chunk_number++;
    return true;
  };

  if (!open_new_chunk()) {
    return false;
  }

  while (std::getline(input, line)) {
    size_t line_size = line.size() + 1;

    if (current_chunk_size + line_size > MAX_CHUNK_SIZE) {
      if (!open_new_chunk()) {
        return false;
      }
    }

    output_chunk << line << '\n';
    current_chunk_size += line_size;
  }

  input.close();
  if (output_chunk.is_open()) {
    output_chunk.close();
  }

  std::println("Successfully split {} into {} chunk(s) in directory: {}",
               input_file, chunk_number - 1, output_dir);

  return true;
}

std::vector<Point> get_points(const std::string &filename) {
  std::vector<Point> points;

  std::ifstream file(filename);
  if (!file.is_open()) {
    std::println("Failed to open input file: {}", filename);
    return {};
  }

  std::string header;
  if (!std::getline(file, header)) {
    std::println("Failed to read header from input file");
    return {};
  }

  std::string line, word;
  while (std::getline(file, line)) {
    std::istringstream iss(line);
    Point p;
    std::getline(iss, word, ',');
    p.x = std::stold(word);
    std::getline(iss, word, ',');
    p.y = std::stold(word);
    std::getline(iss, word, ',');
    p.z = std::stold(word);

    points.emplace_back(p);
  }

  return points;
}

bool save_sorted_chunk(const std::vector<Point> &points,
                       const std::string &filename) {
  std::ofstream output_file(filename);

  if (!output_file.is_open()) {
    std::print("Failed to open output file!\n");
    return false;
  }

  // Preserve exactly 13 digits after the decimal point
  output_file << std::fixed << std::setprecision(13);

  output_file << "x, y, z\n";
  for (const auto &p : points) {
    output_file << p.x << ", " << p.y << ", " << p.z << '\n';
  }
  output_file.close();
  return true;
}

void sort_chunks(const std::string &input_chunks_dir,
                 const std::string &output_chunks_dir) {

  for (const auto &chunk_file :
       std::filesystem::directory_iterator(input_chunks_dir)) {
    if (!chunk_file.is_regular_file()) {
      std::print("Unrecognized file detected! Stopping operation.\n");
      return;
    }

    // std::print("{}\n", chunk_file.path().filename().string());

    std::vector<Point> points = get_points(chunk_file.path().string());

    if (points.empty()) {
      std::print("No data provided!\n");
      continue;
    }

    std::sort(points.begin(), points.end(), [](const auto &p1, const auto &p2) {
      if (p1.x != p2.x)
        return p1.x < p2.x;
      return p1.y < p2.y;
    });

    const std::string filename =
        output_chunks_dir + '/' + chunk_file.path().filename().string();

    if (!save_sorted_chunk(points, filename)) {
      std::print("Failed to save sorted chunk!\n");
    }
    std::print("Sorted chunk ({}) saved successfully!\n",
               chunk_file.path().filename().string());
  }
}

int main(int argc, char **argv) {
  if (argc < 2) {
    std::println("Usage: DAM [-s <input_file> <output_chunks_dir>] [-S "
                 "<input_chunks_dir> <output_chunks_dir>]\n-s: split "
                 "input file\n-S: sort chunks");
    return 1;
  }

  bool split_flag = false;
  bool sort_flag = false;
  std::string split_input_file;
  std::string split_output_chunks_dir;
  std::string sort_input_chunks_dir;
  std::string sort_output_chunks_dir;

  // Parse arguments
  int i = 1;
  while (i < argc) {
    std::string arg = argv[i];

    if (arg == "-s") {
      split_flag = true;
      if (i + 2 < argc && argv[i + 1][0] != '-' && argv[i + 2][0] != '-') {
        split_input_file = argv[i + 1];
        split_output_chunks_dir = argv[i + 2];
        i += 3;
      } else {
        std::println(
            "Error: -s flag requires <input_file> <output_chunks_dir>");
        return 1;
      }
    } else if (arg == "-S") {
      sort_flag = true;
      if (i + 2 < argc && argv[i + 1][0] != '-' && argv[i + 2][0] != '-') {
        sort_input_chunks_dir = argv[i + 1];
        sort_output_chunks_dir = argv[i + 2];
        i += 3;
      } else {
        std::println(
            "Error: -S flag requires <input_chunks_dir> <output_chunks_dir>");
        return 1;
      }
    } else if (arg[0] == '-') {
      // Unknown flag
      std::println("Unknown flag: {}", arg);
      return 1;
    } else {
      // Unexpected positional argument
      std::println("Unexpected argument: {}", arg);
      return 1;
    }
  }

  if (split_flag) {
    if (!split_into_chunks(split_input_file, split_output_chunks_dir)) {
      return 1;
    }
  }

  if (sort_flag) {
    sort_chunks(sort_input_chunks_dir, sort_output_chunks_dir);
  }

  if (!split_flag && !sort_flag) {
    std::println("Usage: DAM [-s <input_file> <output_chunks_dir>] [-S "
                 "<input_chunks_dir> <output_chunks_dir>]\n-s: split "
                 "input file\n-S: sort chunks");
    return 0;
  }

  return 0;
}