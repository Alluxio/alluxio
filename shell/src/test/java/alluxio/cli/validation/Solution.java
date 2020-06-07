package alluxio.cli.validation;

import java.util.*;
import java.util.stream.Collectors;

class Solution {
  public boolean makesquare(int[] nums) {
    if (nums == null || nums.length == 0) return false;

    // sum the lengths
    int sum = Arrays.stream(nums).reduce((a, b) -> a + b).getAsInt();
    if (sum == 0 || sum % 4 != 0)
      return false;
    int length = sum / 4;
    boolean[] flags = new boolean[nums.length];
    Arrays.sort(nums);
    reverse(nums);

    return dfs(0, 0, length, nums, flags);
  }

  public boolean dfs(int sideIdx, int curLength, int targetLength, int[] nums, boolean[] flags) {
    if (curLength == targetLength) {
      sideIdx++;
      curLength = 0;
    }
    if (sideIdx == 4) return true;

    for (int i = 0; i < nums.length; i++) {
      if (!flags[i] && curLength + nums[i] <= targetLength) {
        flags[i] = true;
        if (dfs(sideIdx, curLength + nums[i], targetLength, nums, flags))
          return true;
        flags[i] = false;
        if (curLength == 0) return false;
        if (curLength + nums[i] == targetLength) return false;
        while (i + 1 < nums.length && nums[i + 1] == nums[i]) i++;
      }
    }
    return false;
  }

  public void reverse(int[] nums) {
    int len = nums.length;
    for (int i = 0; i < nums.length / 2; i++) {
      int temp = nums[i];
      nums[i] = nums[len - i - 1];
      nums[len - i - 1] = temp;
    }
  }
}