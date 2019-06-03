// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Text;

namespace Microsoft.StreamProcessing
{
    internal abstract class CommonBaseTemplate
    {
        public abstract string TransformText();
        #region Fields
        private bool endsWithNewline;
        #endregion
        #region Properties

        /// <summary>
        /// The string builder that generation-time code is using to assemble generated output
        /// </summary>
        protected StringBuilder GenerationEnvironment { get; } = new StringBuilder();

        /// <summary>
        /// A list of the lengths of each indent that was added with PushIndent
        /// </summary>
        private List<int> IndentLengths { get; } = new List<int>();

        /// <summary>
        /// Gets the current indent we use when adding lines to the output
        /// </summary>
        public string CurrentIndent { get; private set; } = string.Empty;
        #endregion
        #region Transform-time helpers

        /// <summary>
        /// Write text directly into the generated output
        /// </summary>
        public void Write(string textToAppend)
        {
            if (string.IsNullOrEmpty(textToAppend)) return;

            // If we're starting off, or if the previous text ended with a newline,
            // we have to append the current indent first.
            if ((this.GenerationEnvironment.Length == 0) || this.endsWithNewline)
            {
                this.GenerationEnvironment.Append(this.CurrentIndent);
                this.endsWithNewline = false;
            }

            // Check if the current text ends with a newline
            if (textToAppend.EndsWith(Environment.NewLine, StringComparison.CurrentCulture))
            {
                this.endsWithNewline = true;
            }

            // This is an optimization. If the current indent is "", then we don't have to do any
            // of the more complex stuff further down.
            if (this.CurrentIndent.Length == 0)
            {
                this.GenerationEnvironment.Append(textToAppend);
                return;
            }

            // Everywhere there is a newline in the text, add an indent after it
            textToAppend = textToAppend.Replace(Environment.NewLine, (Environment.NewLine + this.CurrentIndent));

            // If the text ends with a newline, then we should strip off the indent added at the very end
            // because the appropriate indent will be added when the next time Write() is called
            if (this.endsWithNewline)
                this.GenerationEnvironment.Append(textToAppend, 0, (textToAppend.Length - this.CurrentIndent.Length));
            else
                this.GenerationEnvironment.Append(textToAppend);
        }

        /// <summary>
        /// Write text directly into the generated output
        /// </summary>
        public void WriteLine(string textToAppend)
        {
            Write(textToAppend);
            this.GenerationEnvironment.AppendLine();
            this.endsWithNewline = true;
        }

        /// <summary>
        /// Write formatted text directly into the generated output
        /// </summary>
        public void Write(string format, params object[] args)
            => Write(string.Format(CultureInfo.CurrentCulture, format, args));

        /// <summary>
        /// Write formatted text directly into the generated output
        /// </summary>
        public void WriteLine(string format, params object[] args)
            => WriteLine(string.Format(CultureInfo.CurrentCulture, format, args));

        /// <summary>
        /// Increase the indent
        /// </summary>
        public void PushIndent(string indent)
        {
            this.CurrentIndent += indent ?? throw new ArgumentNullException(nameof(indent));
            this.IndentLengths.Add(indent.Length);
        }

        /// <summary>
        /// Remove the last indent that was added with PushIndent
        /// </summary>
        public string PopIndent()
        {
            string returnValue = string.Empty;
            if (this.IndentLengths.Count > 0)
            {
                int indentLength = this.IndentLengths[this.IndentLengths.Count - 1];
                this.IndentLengths.RemoveAt(this.IndentLengths.Count - 1);
                if (indentLength > 0)
                {
                    returnValue = this.CurrentIndent.Substring(this.CurrentIndent.Length - indentLength);
                    this.CurrentIndent = this.CurrentIndent.Remove(this.CurrentIndent.Length - indentLength);
                }
            }
            return returnValue;
        }

        /// <summary>
        /// Remove any indentation
        /// </summary>
        public void ClearIndent()
        {
            this.IndentLengths.Clear();
            this.CurrentIndent = string.Empty;
        }
        #endregion
        #region ToString Helpers

        /// <summary>
        /// Utility class to produce culture-oriented representation of an object as a string.
        /// </summary>
        public sealed class ToStringInstanceHelper
        {
            private IFormatProvider formatProviderField = CultureInfo.InvariantCulture;

            /// <summary>
            /// Gets or sets format provider to be used by ToStringWithCulture method.
            /// </summary>
            public IFormatProvider FormatProvider
            {
                get => this.formatProviderField;
                set
                {
                    if (value != null) this.formatProviderField = value;
                }
            }

            /// <summary>
            /// This is called from the compile/run appdomain to convert objects within an expression block to a string
            /// </summary>
            public string ToStringWithCulture(object objectToConvert)
            {
                if (objectToConvert == null) throw new ArgumentNullException(nameof(objectToConvert));

                var t = objectToConvert.GetType();
                var method = t.GetTypeInfo().GetMethod("ToString", new Type[] { typeof(IFormatProvider) });

                return method == null
                    ? objectToConvert.ToString()
                    : (string)method.Invoke(objectToConvert, new object[] { this.formatProviderField });
            }
        }

        /// <summary>
        /// Helper to produce culture-oriented representation of an object as a string
        /// </summary>
        public ToStringInstanceHelper ToStringHelper { get; } = new ToStringInstanceHelper();
        #endregion
    }
}