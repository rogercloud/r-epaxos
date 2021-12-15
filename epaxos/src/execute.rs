use std::{
    cmp::Ordering::{Equal, Greater, Less},
    collections::{HashMap, VecDeque},
};

use petgraph::{
    algo::tarjan_scc,
    graph::{DiGraph, NodeIndex},
};
use tokio::sync::mpsc;

use crate::types::{Command, CommandExecutor, InstanceSpace, InstanceStatus, SharedInstance};

pub(crate) struct Executor<C, E>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C>,
{
    space: InstanceSpace<C>,
    cmd_exe: E,
}

impl<C, E> Executor<C, E>
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(space: InstanceSpace<C>, cmd_exe: E) -> Self {
        Self { space, cmd_exe }
    }
    // TODO:
    // Wait missing instance and invalid state instance
    pub(crate) async fn execute(&self, mut recv: mpsc::Receiver<SharedInstance<C>>) {
        // A inifite loop to pull instance to execute
        loop {
            let ins = recv.recv().await;

            if ins.is_none() {
                // The channel has been closed, stop recving.
                break;
            }

            let space_clone = Clone::clone(&self.space);
            let executor_clone = Clone::clone(&self.cmd_exe);
            tokio::spawn(async move {
                let (scc, g) = build_scc(space_clone, ins.unwrap()).await;
                execute(scc, g, executor_clone).await;
            });
        }
    }
}

async fn execute<C, E>(scc: Vec<Vec<NodeIndex>>, g: DiGraph<SharedInstance<C>, ()>, cmd_exe: E)
where
    C: Command + Clone + Send + Sync + 'static,
    E: CommandExecutor<C> + Clone + Send + Sync,
{
    for each_scc in scc {
        let ins_vec = each_scc.iter().map(|index| &g[*index]);

        let mut sort_vec = Vec::with_capacity(each_scc.len());
        for (id, ins) in ins_vec.enumerate() {
            let ins_read = ins.get_instance_read().await;
            let ins_read = ins_read.as_ref().unwrap();
            sort_vec.push((id, (ins_read.id.replica, ins_read.seq)));
        }

        sort_vec.sort_by(|a, b| {
            match a.1 .1.partial_cmp(&b.1 .1) {
                Some(Greater) => return Greater,
                Some(Less) => return Less,
                _ => {}
            };

            match a.1 .0.partial_cmp(&b.1 .0) {
                Some(Greater) => Greater,
                Some(Less) => Less,
                _ => Equal,
            }
        });

        for (id, _) in sort_vec {
            let ins = &g[each_scc[id]];
            let mut ins_write = ins.get_instance_write().await;
            let ins_write = ins_write.as_mut().unwrap();

            // It may be executed by other execution tasks
            if matches!(ins_write.status, InstanceStatus::Committed) {
                for c in &ins_write.cmds {
                    // FIXME: handle execute error
                    let _ = c.execute(&cmd_exe).await;
                }
                ins_write.status = InstanceStatus::Executed;
            }
        }
    }
}

async fn build_scc<C>(
    space: InstanceSpace<C>,
    ins: SharedInstance<C>,
) -> (Vec<Vec<NodeIndex>>, DiGraph<SharedInstance<C>, ()>)
where
    C: Command + Clone + Send + Sync + 'static,
{
    let mut queue = VecDeque::new();
    queue.push_back(ins);

    let mut map = HashMap::<SharedInstance<C>, NodeIndex>::new();
    let mut g = DiGraph::<SharedInstance<C>, ()>::new();
    loop {
        let cur = queue.pop_front();

        // queue is empty
        if cur.is_none() {
            break;
        }
        let cur = cur.unwrap();

        // get node index
        let cur_index = get_index(&mut map, &mut g, &cur);
        let cur_read = cur.get_instance_read().await;
        let cur_read_inner = cur_read.as_ref().unwrap();

        for (r, d) in cur_read_inner.deps.iter().enumerate() {
            if d.is_none() {
                continue;
            }
            let r = r.into();
            let d = d.as_ref().unwrap();

            let (d_ins, notify) = space.get_instance_or_notify(&r, d).await;

            let d_ins = if let Some(n) = notify {
                n.notified().await;
                space.get_instance(&r, d).await
            } else {
                d_ins
            };

            assert!(
                d_ins.is_some(),
                "instance should not be none after notification"
            );
            let d_ins = d_ins.unwrap();
            let d_index = get_index(&mut map, &mut g, &d_ins);
            g.add_edge(cur_index, d_index, ());
        }
    }

    (tarjan_scc(&g), g)
}

fn get_index<C>(
    map: &mut HashMap<SharedInstance<C>, NodeIndex>,
    g: &mut DiGraph<SharedInstance<C>, ()>,
    ins: &SharedInstance<C>,
) -> NodeIndex
where
    C: Command + Clone + Send + Sync + 'static,
{
    if !HashMap::contains_key(map, ins) {
        let index = g.add_node(ins.clone());
        map.insert(ins.clone(), index);
        index
    } else {
        *map.get(ins).unwrap()
    }
}
