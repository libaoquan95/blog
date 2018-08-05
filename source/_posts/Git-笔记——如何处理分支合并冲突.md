---
title: Git 笔记——如何处理分支合并冲突
date: 2018-08-04 21:50:17
tags:
---
## 1.前言
学习使用 Git 也有一段时间，但一直都是把 Git 当作一个代码仓库，使用的命令无非就是 clone, add, commit ，往往课程作业也没有过多人合作开发，没有体验过 Git 的分支操作。
但在实习时，我了解到在实际的团队开发中，一个新的功能往往都是在分支中进行开发，最终将开发好的代码合并到 master 中。
合并（merging）是在形式上整合别的分支到你当前的工作分支的操作。Git 所带来的最伟大的改善就是它让合并操作变得非常轻松简单。在大多数情况下，Git 会自己弄清楚该如何整合这些新来的变化。
当然，也存在极少数的情况，你必须自己手动地告诉 Git 该怎么做。最为常见的就是大家都改动了同一个文件。即便在这种情况下，Git 还是有可能自动地发现并解决掉这些冲突。但是，如果两个人同时更改了同一个文件的同一行代码，或者一个人改动了那些被另一个人删除了的代码，Git 就不能简单地确定到底谁的改动才是正确的。这时 Git 会把这些地方标记为一个冲突，你必须首先解决掉这些冲突，然后再继续你的工作。
##  2.出现冲突
首先在主分支 master 上，又一个 readme 文件，内容如下：
```
this is a readme file.
```
接着新建一个分支，并且修改这个 readme 文件。
```
# 新建分支并转到这个分支
$ git checkout -b bran1
Switched to a new branch 'bran1'
```
修改 readme 文件如下：
```
bran1: head
this is a readme file.
bran1: foot
```
之后提交修改
```
$ git add readme
$ git commit -m "在bran1分支下修改"
```
切换到 master 分支
```
$ git checkout master
Switched to branch 'master'
Your branch is ahead of 'origin/master' by 1 commit.
  (use "git push" to publish your local commits)
```
在 master 分支上到 readme 文件，处于上一次修改之前，如果这时候将两个分支合并，是不会出现问题的。但是，如果 master 现在也要修改 readme 文件，就会出现冲突了。
修改 readme 文件如下：
```
master: head
this is a readme file.
master: foot
```
提交修改
```
$ git add readme
$ git commit -m "在master分支下修改"
```
现在 master 分支与 bran1 分支都比文章开始时都 master 分支多走了一步。这种情况下，Git无法执行“快速合并”，只能试图把各自的修改合并起来，但这种合并就可能会有冲突。
```
$ git merge bran1
Auto-merging readme
CONFLICT (content): Merge conflict in readme
Automatic merge failed; fix conflicts and then commit the result.
```
##  3.冲突处理
出现了 Git 无法自动处理的冲突，那为了解决这种冲突，只能手动进行处理，处理方式是在发生冲突的地方进行多选一，只保留一个，那么就可以解决冲突。
此时打开 readme 文件，发现文件内容入下：
```
<<<<<<< HEAD
master: head
=======
bran1: head
>>>>>>> bran1
this is a readme file.
<<<<<<< HEAD
master: foot
=======
bran1: foot
>>>>>>> bran1
```
这里的 <<<<<<< ，=======，>>>>>>> 标记了冲突出现的位置，我们可以对冲突对文本进行选择，比如第一个冲突我们保留 master 下对修改，第二个冲突保留 bran1 下对修改：
```
master: head
this is a readme file.
bran1: foot
```
再次提交
```
$ git add readme
$ git commit -m "conflict fixed"
```
此时冲突已经解决，提交不会出现问题。