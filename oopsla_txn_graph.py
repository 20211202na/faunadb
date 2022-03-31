# elle jepson画图
import copy
from gettext import find
from tkinter import N

class DiGraph:
    def __init__(self):
        self.adj_map = {}
        self.visited = []
        self.trace = []
        self.boo_cycle = False

    def add_edge(self, from_node, to_node):
        if from_node in self.adj_map:
            self.adj_map[from_node].add(to_node)
        else:
            self.adj_map[from_node] = {to_node}

    def add_edges(self, from_node, to_nodes):
        if from_node not in self.adj_map:
            self.adj_map[from_node] = set()
        for to_node in to_nodes:
            self.adj_map[from_node].add(to_node)

    def add_vertex(self, new_node):
        if new_node not in self.adj_map:
            self.adj_map[new_node] = set()

    def has_edge(self, from_node, to_node):
        if from_node in self.adj_map and to_node in self.adj_map[from_node]:
            return True
        else:
            return False

    def has_cycle(self):
        for key in list(self.adj_map.keys()):
            reachable = set()
            if self.dfs_util_reach(key, key, reachable):
                print("reach key is: " + str(key))
                return True
        return False

    def find_cycle(self,start_node):
        if start_node in self.visited:
            if start_node in self.trace:
                self.boo_cycle = True
                trace_index = self.trace.index(start_node)
                for i in range(trace_index, len(self.trace)):
                    print(str(self.trace[i]) + ' ', end='')
                print('\n', end='')
                return
            return
        self.visited.append(start_node)
        self.trace.append(start_node)

        if start_node != '' :
            for node in self.adj_map[start_node]:
                if node in self.adj_map:
                    self.find_cycle(node)
        self.trace.pop()

    def dfs_util_reach(self, s, u, reachable):
        if u in self.adj_map:
            for node in self.adj_map[u]:
                if node == s:
                    return True
                elif node in reachable:
                    continue
                else:
                    reachable.add(node)
                    if self.dfs_util_reach(s, node, reachable):
                        return True
        return False

    def dfs_util_all(self, u, reachable):
        if u in self.adj_map:
            for node in self.adj_map[u]:
                if node in reachable:
                    continue
                reachable.add(node)
                self.dfs_util_all(node, reachable)

    def take_closure(self):
        clone_map = self.adj_map.copy()
        for node in self.adj_map:
            reachable = set()
            self.dfs_util_all(node, reachable)
            clone_map[node] = reachable
        self.adj_map = clone_map

    def union_with(self, g):
        for key, value in g.adj_map.items():
            if key in self.adj_map:
                self.adj_map[key] = self.adj_map[key].union(value)
            else:
                self.adj_map[key] = value


class OopslaAtomicHistoryPO:
    def __init__(self, ops):
        self.so = DiGraph()
        self.vis = DiGraph()
        self.wr_rel = {}
        self.txns = {}
        client_in_so = {}
        self.r_nodes = {}
        self.w_nodes = {}
        current_tra = []
        # Add ops in the type an array of dicts: [{'op_type': 'w', 'var': 1, 'val': 1, 'client_id': 1, 'tra_id': 1}, ...]
        for i in range(len(ops)):
            op_dict = self.get_op(ops[i])
            #for the last op in each transaction
            if i == len(ops) - 1 or self.get_op(ops[i + 1])['tra_id'] != op_dict['tra_id']:
                if op_dict['client_id'] in client_in_so:
                    #add edge between transactions from same client to self.so
                    self.so.add_edge(client_in_so[op_dict['client_id']], op_dict['tra_id']) 
                client_in_so[op_dict['client_id']] = op_dict['tra_id']
                current_tra.append(op_dict)
                
                for op in current_tra:
                    if op['op_type'] == 'w':
                        # if write, if key dont have graph create one and add tra_in as vertex in wr_rel
                        if op['var'] in self.wr_rel:
                            self.wr_rel[op['var']].add_vertex(op_dict['tra_id'])
                        else:
                            graph = DiGraph()
                            graph.add_vertex(op_dict['tra_id'])
                            self.wr_rel[op['var']] = graph

                        # find the corresponding read op and add edge in wl_rel
                        if op['var'] in self.r_nodes:
                            for key in self.r_nodes[op['var']]:
                                # r_nodes[op['var']] record the txn_id that read on var
                                if key != op_dict['tra_id']:
                                    for node in self.txns[key]:
                                        if node['val'] == op['val'] and node['var'] == op['var'] and node[
                                            'op_type'] == 'r':
                                            self.wr_rel[op['var']].add_edge(op_dict['tra_id'], key)
                                            break
                        if op['var'] not in self.w_nodes:
                            self.w_nodes[op['var']] = set()
                        # add the tra_id into w_node[op['var']]
                        self.w_nodes[op['var']].add(op_dict['tra_id'])
                    else:
                        if op['var'] in self.wr_rel:
                            # if read, find the corresponding write and add edge in wr_rel
                            has_wr = False
                            for key, t_set in self.wr_rel[op['var']].adj_map.items():
                                if key != op_dict['tra_id']:
                                    for node in self.txns[key]:
                                        if node['val'] == op['val'] and node['var'] == op['var'] and node[
                                            'op_type'] == 'w':
                                            t_set.add(op_dict['tra_id'])
                                            has_wr = True
                                            break
                                    if has_wr:
                                        break

                        if op['var'] not in self.r_nodes:
                            self.r_nodes[op['var']] = set()
                        # add the tra_id into r_node[op['var']]
                        self.r_nodes[op['var']].add(op_dict['tra_id'])
                if op_dict['tra_id'] not in self.txns:
                    self.txns[op_dict['tra_id']] = []
                # add current txn into self.txns
                self.txns[op_dict['tra_id']].extend(current_tra.copy())
                current_tra.clear()
            else:
                current_tra.append(op_dict)
        self.vis = copy.deepcopy(self.so)
        self.so.take_closure()

    def get_op(self, op):
        op = op.strip('\n')
        arr = op[2:-1].split(',')
        return {
            'op_type': op[0],
            'var': arr[0],
            'val': arr[1],
            'client_id': int(arr[2]),
            'tra_id': int(arr[3]),
        }

    def check_uncommitted_write(self,i):
        uncommitted_read = []
        for var, r_t_set in self.r_nodes.items():
            for r_tra_id in r_t_set:
                for r_op in self.txns[r_tra_id]:
                    if r_op['var'] == var and r_op['op_type'] == 'r':
                        find_write = False
                        for w_tra_id in self.w_nodes[r_op['var']]:
                            for w_op in self.txns[w_tra_id]:
                                if r_op['var'] == w_op['var'] and r_op['val'] == w_op['val'] and w_op['op_type'] == 'w':
                                    find_write = True
                                    break
                            if find_write == True:
                                break
                        if find_write == False:
                            uncommitted_read.append(r_op)
                            file = open('output/'+str(i)+'/uncommitted_read.txt','a');
                            file.write(str(r_op['op_type']) + '(' + str(r_op['var']) + ',' + str(r_op['val']) + ',' + str(r_op['client_id']) + ',' + str(r_op['tra_id']) + ')\n')
                            file.close();
        return uncommitted_read


    def check_read_last_write(self,i):
        read_early_write = []
        for var, r_t_set in self.r_nodes.items():
            for r_tra_id in r_t_set:
                for r_op in self.txns[r_tra_id]:
                    if r_op['var'] == var and r_op['op_type'] == 'r':
                        find_write = False
                        for w_tra_id in self.w_nodes[r_op['var']]:
                            for w_op in self.txns[w_tra_id]:
                                if find_write == True and r_op['var'] == w_op['var'] and w_op['op_type'] == 'w':
                                    e_txn = {'var': r_op['var'], 'w_txn': w_op['tra_id'], 'r_txn': r_op['tra_id']}
                                    read_early_write.append(e_txn)
                                    file = open('output/'+str(i)+'/read_early_write.txt','a');
                                    file.write('(' + str(r_op['var']) + ',' + str(w_op['tra_id']) + ',' + str(r_op['tra_id']) + ')\n')
                                    file.close();
                                    break
                                if r_op['var'] == w_op['var'] and r_op['val'] == w_op['val'] and w_op['op_type'] == 'w':
                                    find_write = True
                                    if r_op['tra_id'] == w_op['tra_id']:
                                        r_flag = False
                                        w_flag = False
                                        for op in self.txns[r_op['tra_id']]:
                                            if w_flag == False and op['val'] == r_op['val'] and op['var'] == r_op['var'] and op['op_type'] == 'w':
                                                w_flag = True
                                            if r_flag == False and op['val'] == r_op['val'] and op['var'] == r_op['var'] and op['op_type'] == 'r':
                                                r_flag = True
                                            if w_flag == True and r_flag == False and op['var'] == r_op['var'] and op['val'] != r_op['val'] and op['op_type'] == 'w':
                                                e_txn = {'var': r_op['var'], 'w_txn': w_op['tra_id'], 'r_txn': r_op['tra_id']}
                                                read_early_write.append(e_txn)
                                                file = open('output/'+str(i)+'/read_early_write.txt','a');
                                                file.write('(' + str(r_op['var']) + ',' + str(w_op['tra_id']) + ',' + str(r_op['tra_id']) + ')\n')
                                                file.close();
                                                break
                                        break
                            if find_write == True:
                                break
        return read_early_write

    def check_multi_read(self,i):
        read_multi_val = []
        for var, r_t_set in self.r_nodes.items():
            for r_tra_id in r_t_set:
                r_val = None
                w_var = False
                for r_op in self.txns[r_tra_id]:
                    if w_var == False and r_val != None and r_val != r_op['val'] and r_op['var'] == var and r_op['op_type'] == 'r' :
                        e_txn = {'var': r_op['var'], 'r_txn': r_op['tra_id'], 'r_val_before': r_val, 'r_val_after': r_op['val']}
                        read_multi_val.append(e_txn)
                        file = open('output/'+str(i)+'/read_multi_val.txt','a');
                        file.write('(' + str(r_op['var']) + ',' + str(r_op['tra_id']) + ',' + str(r_val) + ',' + str(r_op['val']) + ')\n')
                        file.close();
                        break
                    if r_op['var'] == var and r_op['op_type'] == 'r':
                        r_val = r_op['val']
                    if r_val != None and r_op['var'] == var and r_op['op_type'] == 'w':
                        w_var = True
                        r_val = None
        return read_multi_val


    def get_wr(self):
        # combine all wr_rel of different var into one graph
        wr = DiGraph()
        for key, digraph in self.wr_rel.items():
            wr.union_with(digraph)
        return wr

    def vis_includes(self, g):
        self.vis.union_with(g)

    def vis_is_trans(self):
        self.vis.take_closure()

    def casual_ww(self):
        ww = {}
        for x, wr_x in self.wr_rel.items():
            ww_x = DiGraph()
            for t1, t3s in wr_x.adj_map.items():
                for t2 in list(wr_x.adj_map):
                    if t1 != t2:
                        has_edge = False
                        if self.vis.has_edge(t2, t1):
                            has_edge = True
                        else:
                            for t3 in t3s:
                                if self.vis.has_edge(t2, t3):
                                    has_edge = True
                                    break
                        if has_edge:
                            ww_x.add_edge(t2, t1)
            ww[x] = ww_x
        return ww

    def check_read_zero(self):
        for key, t_set in self.vis.adj_map.items():
            w_vars = set()
            for node in self.txns[key]:
                if node['op_type'] == 'w':
                    w_vars.add(node['var'])
            for t in t_set:
                for t_node in self.txns[t]:
                    if t_node['op_type'] == 'r' and t_node['val'] == '0' and t_node['var'] in w_vars:
                        return True
        return False


if __name__ == '__main__':
    for i in range(1):
        # folder_name = "output/"+str(i)+"/result.txt"
        folder_name = 'bug1/result.txt'
        with open(folder_name) as in_file:
            raw_ops = in_file.readlines()
        causal_hist = OopslaAtomicHistoryPO(raw_ops)

        # uncommitted_read = causal_hist.check_uncommitted_write(i)
        # print('uncommitted_read' + str(uncommitted_read))
        # read_early_write = causal_hist.check_read_last_write(i)
        # print('read_early_write' + str(read_early_write))
        # read_multi_val = causal_hist.check_multi_read(i)
        # print('read_multi_val' + str(read_multi_val))

        wr = causal_hist.get_wr()
        #so union wr
        causal_hist.vis_includes(wr)
        causal_hist.vis_is_trans()
        if causal_hist.vis.has_cycle():
            print('BP111111 found in: ' + str(i))
            # print(causal_hist.vis.find_cycle(0))

        ww = causal_hist.casual_ww()
        for key, ww_x in ww.items():
            causal_hist.vis_includes(ww_x)
        causal_hist.vis_is_trans()
        # file = open('adjmap.txt','w');
        # file.write(str(causal_hist.vis.adj_map));
        # file.close();
        if causal_hist.vis.has_cycle():
            print('BP222222 found in: ' + str(i))
            # print(causal_hist.vis.find_cycle(7))