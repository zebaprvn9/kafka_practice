package com.basic.kafka.Basic_Kafka.practice;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class TreeNode {
    int val;
    TreeNode left;
    TreeNode right;
    TreeNode() {}
    TreeNode(int val) { this.val = val; }
    TreeNode(int val, TreeNode left, TreeNode right) {
          this.val = val;
          this.left = left;
          this.right = right;
      }
  }
public class BTTraverse {
    public List<List<Integer>> levelOrder(TreeNode root) {

        List<List<Integer>> result = new ArrayList<>();
        traverseTree(root, 0, result);
        return result;        
    }

    public void traverseTree(TreeNode root, int level, List<List<Integer>> result ) {

        if(root == null) {
            return;    
        }
        if(level == result.size()) {
            result.add(new ArrayList<>());
        }
        result.get(level).add(root.val);
        traverseTree(root.left, level+1, result);
        traverseTree(root.right, level+1, result);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        TreeNode treeNode = new TreeNode(3);
        treeNode.left = new TreeNode(9);
        treeNode.right = new TreeNode(20);
        treeNode.left.left = null;
        treeNode.left.right = null;
        treeNode.right.left = new TreeNode(15);
        treeNode.right.right = new TreeNode(7);
        BTTraverse btTraverse = new BTTraverse();
        List<List<Integer>> result = btTraverse.levelOrder(treeNode);
        System.out.print(Arrays.toString(result.toArray()));


    }
}