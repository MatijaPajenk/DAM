#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <print>
#include <sstream>
#include <string>
#include <vector>

constexpr size_t MAX_CHUNK_SIZE = 10 * 1024 * 1024;
constexpr size_t MERGE_BUFFER_SIZE = 5 * 1024 * 1024;

void print_progress(const std::string &operation, size_t current,
                    size_t total) {
  int percentage = total > 0 ? (current * 100 / total) : 0;
  int bar_width = 50;
  int filled = bar_width * current / (total > 0 ? total : 1);

  std::cout << "\r" << operation << " [";
  for (int i = 0; i < bar_width; ++i) {
    if (i < filled)
      std::cout << "=";
    else if (i == filled)
      std::cout << ">";
    else
      std::cout << " ";
  }
  std::cout << "] " << percentage << "% (" << current << "/" << total << ")";
  std::cout.flush();

  if (current >= total) {
    std::cout << std::endl;
  }
}

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

  // Get file size for progress tracking
  input.seekg(0, std::ios::end);
  size_t total_size = input.tellg();
  input.seekg(0, std::ios::beg);

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

  size_t bytes_processed = header.size() + 1;
  size_t lines_processed = 0;
  const size_t update_interval = 10000; // Update progress every 10k lines

  while (std::getline(input, line)) {
    size_t line_size = line.size() + 1;

    if (current_chunk_size + line_size > MAX_CHUNK_SIZE) {
      if (!open_new_chunk()) {
        return false;
      }
    }

    output_chunk << line << '\n';
    current_chunk_size += line_size;
    bytes_processed += line_size;
    lines_processed++;

    if (lines_processed % update_interval == 0) {
      print_progress("Splitting", bytes_processed, total_size);
    }
  }

  // Final progress update
  print_progress("Splitting", total_size, total_size);

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

  // Count total chunks first
  size_t total_chunks = 0;
  for (const auto &chunk_file :
       std::filesystem::directory_iterator(input_chunks_dir)) {
    if (chunk_file.is_regular_file()) {
      total_chunks++;
    }
  }

  size_t processed_chunks = 0;
  for (const auto &chunk_file :
       std::filesystem::directory_iterator(input_chunks_dir)) {
    if (!chunk_file.is_regular_file()) {
      std::print("Unrecognized file detected! Stopping operation.\n");
      return;
    }

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

    processed_chunks++;
    print_progress("Sorting chunks", processed_chunks, total_chunks);
  }
}

std::vector<Point> load_buffer(std::ifstream &file, size_t max_size) {
  std::vector<Point> buffer;
  size_t current_size = 0;
  std::string line, word;

  while (true) {
    const std::streampos line_start = file.tellg();
    if (!std::getline(file, line)) {
      break;
    }

    const size_t line_size = line.size() + 1;
    if (current_size + line_size > max_size && !buffer.empty()) {
      file.clear();
      file.seekg(line_start);
      break;
    }

    std::istringstream iss(line);
    Point p;
    std::getline(iss, word, ',');
    p.x = std::stold(word);
    std::getline(iss, word, ',');
    p.y = std::stold(word);
    std::getline(iss, word, ',');
    p.z = std::stold(word);

    buffer.emplace_back(p);
    current_size += line_size;
  }

  return buffer;
}

bool merge_two_chunks(const std::string &chunk1_path,
                      const std::string &chunk2_path,
                      const std::string &output_path) {
  std::ifstream file1(chunk1_path);
  std::ifstream file2(chunk2_path);
  std::ofstream output_file(output_path);

  if (!file1.is_open() || !file2.is_open() || !output_file.is_open()) {
    std::println("Failed to open files for merging");
    return false;
  }

  std::string header;
  std::getline(file1, header);
  std::getline(file2, header);
  output_file << "x, y, z\n";
  output_file << std::fixed << std::setprecision(13);

  std::vector<Point> buffer1 = load_buffer(file1, MERGE_BUFFER_SIZE);
  std::vector<Point> buffer2 = load_buffer(file2, MERGE_BUFFER_SIZE);
  size_t idx1 = 0, idx2 = 0;

  while (true) {
    if (idx1 >= buffer1.size() && file1.good()) {
      buffer1 = load_buffer(file1, MERGE_BUFFER_SIZE);
      idx1 = 0;
    }
    if (idx2 >= buffer2.size() && file2.good()) {
      buffer2 = load_buffer(file2, MERGE_BUFFER_SIZE);
      idx2 = 0;
    }

    if (idx1 >= buffer1.size() && idx2 >= buffer2.size()) {
      break;
    }

    if (idx1 < buffer1.size() && idx2 < buffer2.size()) {
      const Point &p1 = buffer1[idx1];
      const Point &p2 = buffer2[idx2];

      if (p1.x < p2.x || (p1.x == p2.x && p1.y < p2.y)) {
        output_file << p1.x << ", " << p1.y << ", " << p1.z << '\n';
        idx1++;
      } else {
        output_file << p2.x << ", " << p2.y << ", " << p2.z << '\n';
        idx2++;
      }
    } else if (idx1 < buffer1.size()) {
      const Point &p1 = buffer1[idx1];
      output_file << p1.x << ", " << p1.y << ", " << p1.z << '\n';
      idx1++;
    } else if (idx2 < buffer2.size()) {
      const Point &p2 = buffer2[idx2];
      output_file << p2.x << ", " << p2.y << ", " << p2.z << '\n';
      idx2++;
    }
  }

  file1.close();
  file2.close();
  output_file.close();
  return true;
}

void merge_chunks(const std::string &chunks_dir,
                  const std::string &output_dir) {
  std::vector<std::string> chunk_files;

  // Collect all chunk files
  for (const auto &entry : std::filesystem::directory_iterator(chunks_dir)) {
    if (entry.is_regular_file()) {
      chunk_files.push_back(entry.path().string());
    }
  }

  if (chunk_files.empty()) {
    std::println("No chunk files found in {}", chunks_dir);
    return;
  }

  // Sort filenames for consistent ordering
  std::sort(chunk_files.begin(), chunk_files.end());

  std::println("Merging {} chunks...", chunk_files.size());

  // Calculate total merge operations
  size_t total_merges = chunk_files.size() - 1; // n chunks need n-1 merges
  size_t completed_merges = 0;

  // Merge chunks iteratively
  std::vector<std::string> current_level = chunk_files;
  int merge_iteration = 0;

  while (current_level.size() > 1) {
    std::vector<std::string> next_level;
    merge_iteration++;

    for (size_t i = 0; i < current_level.size(); i += 2) {
      if (i + 1 < current_level.size()) {
        // Merge pair of chunks
        std::string output_name = output_dir + "/merged_" +
                                  std::to_string(merge_iteration) + "_" +
                                  std::to_string(i / 2) + ".csv";
        if (merge_two_chunks(current_level[i], current_level[i + 1],
                             output_name)) {
          next_level.push_back(output_name);
          completed_merges++;
          print_progress("Merging chunks", completed_merges, total_merges);
        }
      } else {
        // Odd chunk out, move to next level
        next_level.push_back(current_level[i]);
      }
    }
    current_level = next_level;
  }

  if (!current_level.empty()) {
    std::string final_output = output_dir + "/merged_final.csv";
    std::filesystem::rename(current_level[0], final_output);
    std::println("Final merged result: {}", final_output);
  }
}

int main(int argc, char **argv) {
  if (argc < 2) {
    std::println("Usage: DAM [-s <input_file> <output_chunks_dir>] [-S "
                 "<input_chunks_dir> <output_chunks_dir>] [-m "
                 "<input_chunks_dir> <output_merge_dir>]\n-s: split "
                 "input file\n-S: sort chunks\n-m: merge chunks");
    return 0;
  }

  bool split_flag = false;
  bool sort_flag = false;
  bool merge_flag = false;
  std::string split_input_file;
  std::string split_output_chunks_dir;
  std::string input_chunks_dir;
  std::string sort_output_chunks_dir;
  std::string merge_output_dir;

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
        input_chunks_dir = argv[i + 1];
        sort_output_chunks_dir = argv[i + 2];
        i += 3;
      } else {
        std::println(
            "Error: -S flag requires <input_chunks_dir> <output_chunks_dir>");
        return 1;
      }
    } else if (arg == "-m") {
      merge_flag = true;
      if (i + 2 < argc && argv[i + 1][0] != '-' && argv[i + 2][0] != '-') {
        input_chunks_dir = argv[i + 1];
        merge_output_dir = argv[i + 2];
        i += 3;
      } else {
        std::println(
            "Error: -m flag requires <input_chunks_dir> <output_merge_dir>");
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
    sort_chunks(input_chunks_dir, sort_output_chunks_dir);
  }

  if (merge_flag) {
    merge_chunks(input_chunks_dir, merge_output_dir);
  }

  if (!split_flag && !sort_flag && !merge_flag) {
    std::println("Usage: DAM [-s <input_file> <output_chunks_dir>] [-S "
                 "<input_chunks_dir> <output_chunks_dir>] [-m "
                 "<input_chunks_dir> <output_merge_dir>]\n-s: split "
                 "input file\n-S: sort chunks\n-m: merge chunks");
  }

  return 0;
}