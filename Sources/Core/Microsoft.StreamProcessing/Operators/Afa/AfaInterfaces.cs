// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TRegister"></typeparam>
    /// <typeparam name="TAccumulator"></typeparam>
    public interface IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>
    {
        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="pattern"></param>
        /// <param name="patterns"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> Concat(Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern, params Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>>[] patterns);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="edit"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> Edit(Action<Afa<TPayload, TRegister, TAccumulator>> edit);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> Epsilon();

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="pattern"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> KleenePlus(Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="pattern"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> KleeneStar(Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="initialize"></param>
        /// <param name="accumulate"></param>
        /// <param name="skipToEnd"></param>
        /// <param name="fence"></param>
        /// <param name="transfer"></param>
        /// <param name="dispose"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> MultiElement(Expression<Func<long, TRegister, TAccumulator>> initialize, Expression<Func<long, TPayload, TRegister, TAccumulator, TAccumulator>> accumulate, Expression<Func<long, TPayload, TAccumulator, bool>> skipToEnd, Expression<Func<long, TAccumulator, TRegister, bool>> fence, Expression<Func<long, TAccumulator, TRegister, TRegister>> transfer, Expression<Action<TAccumulator>> dispose);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="pattern1"></param>
        /// <param name="pattern2"></param>
        /// <param name="patterns"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> Or(Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern1, Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern2, params Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>>[] patterns);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="pattern1"></param>
        /// <param name="pattern2"></param>
        /// <param name="patterns"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> OrConcat(Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern1, Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern2, params Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>>[] patterns);

        #region SingleElement

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<long, TPayload, TRegister, bool>> condition = null, Expression<Func<long, TPayload, TRegister, TRegister>> aggregator = null);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<long, TPayload, TRegister, bool>> condition, Expression<Func<TPayload, TRegister, TRegister>> aggregator);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<long, TPayload, TRegister, bool>> condition, Expression<Func<TPayload, TRegister>> aggregator);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, bool>> condition, Expression<Func<long, TPayload, TRegister, TRegister>> aggregator = null);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, bool>> condition, Expression<Func<TPayload, TRegister, TRegister>> aggregator);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, bool>> condition, Expression<Func<TPayload, TRegister>> aggregator);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, TRegister, bool>> condition, Expression<Func<long, TPayload, TRegister, TRegister>> aggregator = null);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, TRegister, bool>> condition, Expression<Func<TPayload, TRegister, TRegister>> aggregator);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, TRegister, bool>> condition, Expression<Func<TPayload, TRegister>> aggregator);
        #endregion

        #region ListElement

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<long, List<TPayload>, TRegister, bool>> condition = null, Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregator = null);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<long, List<TPayload>, TRegister, bool>> condition, Expression<Func<List<TPayload>, TRegister, TRegister>> aggregator);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<long, List<TPayload>, TRegister, bool>> condition, Expression<Func<List<TPayload>, TRegister>> aggregator);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, bool>> condition, Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregator = null);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, bool>> condition, Expression<Func<List<TPayload>, TRegister, TRegister>> aggregator);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, bool>> condition, Expression<Func<List<TPayload>, TRegister>> aggregator);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, TRegister, bool>> condition, Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregator = null);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, TRegister, bool>> condition, Expression<Func<List<TPayload>, TRegister, TRegister>> aggregator);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="aggregator"></param>
        /// <returns></returns>
        IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, TRegister, bool>> condition, Expression<Func<List<TPayload>, TRegister>> aggregator);
        #endregion
    }

    /// <summary>
    /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TRegister"></typeparam>
    /// <typeparam name="TAccumulator"></typeparam>
    public interface IAbstractPattern<TKey, TPayload, TRegister, TAccumulator> : IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>
    {
        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <typeparam name="TAccumulatorNew"></typeparam>
        /// <param name="defaultAccumulator"></param>
        /// <returns></returns>
        IAbstractPattern<TKey, TPayload, TRegister, TAccumulatorNew> SetAccumulator<TAccumulatorNew>(TAccumulatorNew defaultAccumulator = default);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <typeparam name="TRegisterNew"></typeparam>
        /// <param name="defaultRegister"></param>
        /// <returns></returns>
        IAbstractPattern<TKey, TPayload, TRegisterNew, TAccumulator> SetRegister<TRegisterNew>(TRegisterNew defaultRegister = default);

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="pattern"></param>
        /// <param name="maxDuration"></param>
        /// <param name="allowOverlappingInstances"></param>
        /// <param name="isDeterministic"></param>
        /// <returns></returns>
        IStreamable<TKey, TRegister> Detect(Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern, long maxDuration = 0, bool allowOverlappingInstances = true, bool isDeterministic = false);
    }

    /// <summary>
    /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TRegister"></typeparam>
    /// <typeparam name="TAccumulator"></typeparam>
    public interface IPattern<TKey, TPayload, TRegister, TAccumulator> : IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>
    {
        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        Afa<TPayload, TRegister, TAccumulator> AFA { get; }

        /// <summary>
        /// Interface is only public to serve CLR strong typing - instances should only be created internally. Do not use directly.
        /// </summary>
        /// <param name="maxDuration"></param>
        /// <param name="allowOverlappingInstances"></param>
        /// <param name="isDeterministic"></param>
        /// <returns></returns>
        IStreamable<TKey, TRegister> Detect(long maxDuration = 0, bool allowOverlappingInstances = true, bool isDeterministic = false);
    }
}
