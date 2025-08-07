package com.basic.kafka.Basic_Kafka.practice;

import java.io.*;
import java.util.Base64;
import java.util.Stack;

class TreeNodeClass implements Serializable {
    int val;
    TreeNodeClass left;
    TreeNodeClass right;

}

public class Codec {


    // Encodes a tree to a single string.
    public String serialize(TreeNodeClass root) {

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream aos = new ObjectOutputStream(bos);
            aos.writeObject(root);
            aos.close();
            return Base64.getEncoder().encodeToString(bos.toByteArray());
        } catch(Exception e) {
            System.out.println(e);
        }
        return null;
    }

    // Decodes your encoded data to tree.
    public TreeNodeClass deserialize(String data)   {

        try {

        ByteArrayInputStream bis = new ByteArrayInputStream(Base64.getDecoder().decode(data));
        ObjectInputStream ais = new ObjectInputStream(bis);
        return (TreeNodeClass)ais.readObject();

        } catch(ClassNotFoundException  | IOException e) {
            System.out.println(e);
        }
        return null;
    }

    public static void main(String[] args) {

        Stack<Integer> stack = new Stack<>();

        TreeNodeClass treeNodeClass = new TreeNodeClass();
        treeNodeClass.val = 10;
        Codec ser = new Codec();
        ser.serialize(treeNodeClass);

        TreeNodeClass ans = ser.deserialize(ser.serialize(treeNodeClass));

        System.out.print(ans.val);
    }
}

// Your Codec object will be instantiated and called as such:
// Codec ser = new Codec();
// Codec deser = new Codec();
// TreeNode ans = deser.deserialize(ser.serialize(root));