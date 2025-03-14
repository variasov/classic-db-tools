from ast import *

import falcon

Module(body=[FunctionDef(name='test_map',
                         args=arguments(posonlyargs=[], args=[arg(arg='rows')],
                                        kwonlyargs=[], kw_defaults=[],
                                        defaults=[]), body=[
        Assign(targets=[Name(id='cakes', ctx=Store())],
               value=Dict(keys=[], values=[])),
        Assign(targets=[Name(id='dudes', ctx=Store())],
               value=Dict(keys=[], values=[])),
        For(target=Name(id='row', ctx=Store()),
            iter=Name(id='rows', ctx=Load()), body=[
                Assign(targets=[Name(id='cake', ctx=Store())], value=Call(
                    func=Attribute(value=Name(id='cakes', ctx=Load()),
                                   attr='get', ctx=Load()), args=[
                        Subscript(value=Name(id='row', ctx=Load()),
                                  slice=Constant(value='id'), ctx=Load())],
                    keywords=[])),
                If(test=Compare(left=Name(id='cake', ctx=Load()), ops=[Is()],
                                comparators=[Constant(value=None)]), body=[
                    Assign(targets=[Name(id='cake', ctx=Store()), Subscript(
                        value=Name(id='cakes', ctx=Load()),
                        slice=Subscript(value=Name(id='row', ctx=Load()),
                                        slice=Constant(value='id'), ctx=Load()),
                        ctx=Store())], value=Dict(
                        keys=[Constant(value='id'), Constant(value='name'),
                              Constant(value='slices'),
                              Constant(value='eaten_by')], values=[
                            Subscript(value=Name(id='row', ctx=Load()),
                                      slice=Constant(value='id'), ctx=Load()),
                            Subscript(value=Name(id='row', ctx=Load()),
                                      slice=Constant(value='name'), ctx=Load()),
                            Subscript(value=Name(id='row', ctx=Load()),
                                      slice=Constant(value='slices'),
                                      ctx=Load()),
                            List(elts=[], ctx=Load())]))], orelse=[]),
                Assign(targets=[Name(id='dude', ctx=Store())], value=Call(
                    func=Attribute(value=Name(id='dudes', ctx=Load()),
                                   attr='get', ctx=Load()), args=[
                        Subscript(value=Name(id='row', ctx=Load()),
                                  slice=Constant(value='dude_id'), ctx=Load())],
                    keywords=[])),
                If(test=Compare(left=Name(id='dude', ctx=Load()), ops=[Is()],
                                comparators=[Constant(value=None)]), body=[
                    Assign(targets=[Name(id='dude', ctx=Store()), Subscript(
                        value=Name(id='dudes', ctx=Load()),
                        slice=Subscript(value=Name(id='row', ctx=Load()),
                                        slice=Constant(value='dude_id'),
                                        ctx=Load()), ctx=Store())], value=Dict(
                        keys=[Constant(value='id'), Constant(value='name')],
                        values=[Subscript(value=Name(id='row', ctx=Load()),
                                          slice=Constant(value='dude_id'),
                                          ctx=Load()),
                                Subscript(value=Name(id='row', ctx=Load()),
                                          slice=Constant(value='dude_name'),
                                          ctx=Load())]))], orelse=[]), Expr(
                    value=Call(func=Attribute(
                        value=Subscript(value=Name(id='cake', ctx=Load()),
                                        slice=Constant(value='eaten_by'),
                                        ctx=Load()), attr='append', ctx=Load()),
                               args=[Name(id='dude', ctx=Load())],
                               keywords=[]))], orelse=[]),
        Return(value=Name(id='cakes', ctx=Load()))], decorator_list=[])],
       type_ignores=[])
