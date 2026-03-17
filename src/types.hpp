#pragma once
#include <memory>
#include <optional>
#include <sstream>
#include <string>

struct Point {
  double x, y, z;

  static Point parse(const std::string &line) {
    std::istringstream iss(line);
    std::string word;
    Point p;
    std::getline(iss, word, ',');
    p.x = std::stod(word);
    std::getline(iss, word, ',');
    p.y = std::stod(word);
    std::getline(iss, word, ',');
    p.z = std::stod(word);
    return p;
  }
};

struct Bounds {
  double xmin, xmax, ymin, ymax;
};

struct ChildLinks {
  std::string left, right;
};

enum class Axis { X, Y, NONE };

struct NodeMeta {
  std::string node_path;
  int depth = 0;
  bool is_leaf = false;
  Axis axis = Axis::NONE;
  std::optional<double> split_value = std::nullopt;
  Bounds bounds{};
  ChildLinks children{};
};

struct Node {
  std::string path;
  std::string filename;
  double split_value = 0;
  double xmin = 0, xmax = 0, ymin = 0, ymax = 0;
  std::unique_ptr<Node> left;
  std::unique_ptr<Node> right;
  Axis axis = Axis::NONE;
  int depth = 0;
  bool is_leaf = false;
};