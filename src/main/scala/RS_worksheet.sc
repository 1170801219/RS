
val currentIndices = List.range(1, 31).filter(x => x != 2)
val optimalSampleIndex = 2
val l = List.tabulate(currentIndices.length / 2, 3)((x, y) => if (y == 2) optimalSampleIndex else currentIndices.apply(x * 2 + y))
